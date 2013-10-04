package com.thinkaurelius.titan.diskstorage.cassandra;

import com.google.common.base.Joiner;
import com.thinkaurelius.titan.CassandraStorageSetup;
import com.thinkaurelius.titan.core.TitanException;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.TemporaryStorageException;
import com.thinkaurelius.titan.diskstorage.cassandra.utils.CassandraDaemonWrapper;
import com.thinkaurelius.titan.diskstorage.cassandra.thrift.thriftpool.CTConnection;
import com.thinkaurelius.titan.diskstorage.cassandra.thrift.thriftpool.CTConnectionFactory;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Map;
import java.util.concurrent.*;

public class CassandraProcessStarter {
    private Process cassandraProcess;
    private Future<?> cassandraOutputLoggerFuture;
    private CassandraOutputReader outputReader;
    private Thread cassandraKiller;
    private boolean delete = true;
    
    private final ExecutorService cassandraOutputLogger =
            Executors.newSingleThreadExecutor();
    private boolean logCassandraOutput = true;
    private final String address;
    private final String cassandraConfigDir;
    private final String cassandraDataDir;
    private final String cassandraInclude;

    private static final String cassandraCommand = "cassandra";
    private static final int port =
            AbstractCassandraStoreManager.PORT_DEFAULT;
    private static final Logger log =
            LoggerFactory.getLogger(CassandraProcessStarter.class);
    private static final long CASSANDRA_STARTUP_TIMEOUT = 10000L;

    private CassandraProcessStarter(String address) {
        this.address = address;

        cassandraDataDir = StringUtils.join(new String[]{"target",
                "cassandra-tmp", "workdir", address}, File.separator);

        cassandraConfigDir = StringUtils.join(new String[]{"target",
                "cassandra-tmp", "conf", address}, File.separator);

        cassandraInclude = cassandraConfigDir + File.separator
                + "cassandra.in.sh";

        // We rely on maven to provide config files ahead of test execution
        assert (new File(cassandraConfigDir).isDirectory());
        assert (new File(cassandraInclude).isFile());
    }

    public CassandraProcessStarter() {
        this("127.0.0.1");
    }

    public CassandraProcessStarter setDelete(boolean delete) {
        this.delete = delete;
        return this;
    }

    public CassandraProcessStarter setLogging(boolean logging) {
        logCassandraOutput = logging;
        return this;
    }

    public boolean getLogging() {
        return logCassandraOutput;
    }

    public void startCassandra() {
        try {
            final File cdd = new File(cassandraDataDir);

            if (delete) {
                if (cdd.isDirectory()) {
                    log.debug("Deleting dir {}...", cassandraDataDir);
                    FileUtils.deleteQuietly(new File(cassandraDataDir));
                    log.debug("Deleted dir {}", cassandraDataDir);
                } else if (cdd.isFile()) {
                    log.debug("Deleting file {}...", cassandraDataDir);
                    cdd.delete();
                    log.debug("Deleted file {}", cassandraDataDir);
                } else {
                    log.debug("Cassandra data directory {} does not exist; " +
                            "letting Cassandra startup script create it",
                            cassandraDataDir);
                }
            }

            ProcessBuilder pb = new ProcessBuilder(cassandraCommand, "-f");
            Map<String, String> env = pb.environment();
            env.put("CASSANDRA_CONF", cassandraConfigDir);
            env.put("CASSANDRA_INCLUDE", cassandraInclude);
            pb.redirectErrorStream(true);
            // Tail Cassandra
            log.debug("Starting Cassandra process {}...",
                    StringUtils.join(pb.command(), ' '));
            cassandraProcess = pb.start();
            log.debug("Started Cassandra process {}.",
                    StringUtils.join(pb.command(), ' '));
            // Register a Cassandra-killer shutdown hook
            cassandraKiller = new CassandraKiller(cassandraProcess, address + ":" + port);
            Runtime.getRuntime().addShutdownHook(cassandraKiller);

            // Create Runnable to process Cassandra's stderr and stdout
            log.debug("Starting Cassandra output handler task...");
            outputReader = new CassandraOutputReader(cassandraProcess,
                    address, port, logCassandraOutput);
            cassandraOutputLoggerFuture =
                    cassandraOutputLogger.submit(outputReader);
            log.debug("Started Cassandra output handler task.");

            // Block in a loop until connection to the Thrift port succeeds
            long sleep = 0;
            long connectAttemptStartTime = System.currentTimeMillis();
            final long sleepGrowthIncrement = 100;
            log.debug(
                    "Attempting to connect to Cassandra's Thrift service on {}:{}...",
                    address, port);
            while (!Thread.currentThread().isInterrupted()) {
                Socket s = new Socket();
                s.setSoTimeout(50);
                try {
                    s.connect(new InetSocketAddress(address, port));
                    long delay = System.currentTimeMillis() -
                            connectAttemptStartTime;
                    log.debug("Thrift connection to {}:{} succeeded " +
                            "(about {} ms after process start)",
                            new Object[]{address, port, delay});
                    break;
                } catch (IOException e) {
                    sleep += sleepGrowthIncrement;
                    log.debug("Thrift connection failed; retrying in {} ms",
                            sleep);
                    Thread.sleep(sleep);
                } finally {
                    if (s.isConnected()) {
                        s.close();
                        log.debug("Closed Thrift connection to {}:{}",
                                address, port);
                    }
                }
            }

			/* 
			 * Check that the Cassandra process logged that it
			 * successfully bound its Thrift port.
			 * 
			 * This detects
			 */
            log.debug("Waiting for Cassandra process to log successful Thrift-port bind...");
            if (!outputReader.awaitThrift(CASSANDRA_STARTUP_TIMEOUT, TimeUnit.MILLISECONDS)) {
                String msg = "Cassandra process failed to bind Thrift-port within timeout.";
                log.error(msg);
                throw new TemporaryStorageException(msg);
            }
            log.debug("Cassandra process logged successful Thrift-port bind.");
        } catch (Exception e) {
            e.printStackTrace();
            throw new TitanException(e);
        }
    }

    public void stopCassandra() {
        try {
            if (null != cassandraOutputLoggerFuture) {
                cassandraOutputLoggerFuture.cancel(true);
                try {
                    cassandraOutputLoggerFuture.get();
                } catch (CancellationException e) {
                }
                cassandraOutputLoggerFuture = null;
            }
            if (null != cassandraProcess) {
                cassandraProcess.destroy();
                cassandraProcess.waitFor();
                cassandraProcess = null;
            }
            if (null != cassandraKiller) {
                Runtime.getRuntime().removeShutdownHook(cassandraKiller);
                cassandraKiller = null;
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new TitanException(e);
        }
    }

    public void waitForClusterSize(int minSize) throws InterruptedException, StorageException {
        CTConnectionFactory f = new CTConnectionFactory(address, port,
                GraphDatabaseConfiguration.CONNECTION_TIMEOUT_DEFAULT,
                AbstractCassandraStoreManager.THRIFT_DEFAULT_FRAME_SIZE);
        CTConnection conn = null;
        try {
            conn = f.makeRawConnection();
            CTConnectionFactory.waitForClusterSize(conn.getClient(), minSize);
        } catch (TTransportException e) {
            throw new TemporaryStorageException(e);
        } finally {
            if (null != conn)
                if (conn.getTransport().isOpen())
                    conn.getTransport().close();
        }
    }

    public static synchronized void startCleanEmbedded(String cassandraYamlPath) {
        if (CassandraDaemonWrapper.isStarted()) {
            log.debug("Already started embedded cassandra; subsequent attempts to start do nothing");
            return;
        }
        
        try {
            FileUtils.deleteDirectory(new File(CassandraStorageSetup.DATA_PATH));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        CassandraDaemonWrapper.start(cassandraYamlPath);
    }
}

class CassandraOutputReader implements Runnable {
    private final Process p;
    private final boolean logAllCassandraOutput;
    private final String address;
    private final int port;
    private final Logger log;

    private static final String THRIFT_BOUND_LOGLINE =
            "Listening for thrift clients...";

    private final CountDownLatch thriftBound = new CountDownLatch(1);

    CassandraOutputReader(Process p, String address, int port, boolean logAllCassandraOutput) {
        this.p = p;
        this.address = address;
        this.port = port;
        this.logAllCassandraOutput = logAllCassandraOutput;
        log = LoggerFactory.getLogger("Cassandra:" + address.replace('.', '-') + ":" + port);
    }

    public boolean awaitThrift(long timeout, TimeUnit units) throws InterruptedException {
        return thriftBound.await(timeout, units);
    }

    @Override
    public void run() {
        InputStream is = p.getInputStream();
        String prefix = null;
        prefix = "[" + address + ":" + port + "]";
        try {
            BufferedReader br = new BufferedReader(
                    new InputStreamReader(is));
            while (!Thread.currentThread().isInterrupted()) {
                String l = br.readLine();
                if (null == l)
                    break;
                if (l.endsWith(THRIFT_BOUND_LOGLINE))
                    thriftBound.countDown();
                if (logAllCassandraOutput)
                    log.debug("{} {}", prefix, l);
            }
            log.debug("Shutdown by interrupt");
        } catch (IOException e) {
            log.debug("IOException: {}", e.getMessage());
        } catch (Exception e) {
            log.debug("Terminated by Exception", e);
        }
    }
}

class CassandraKiller extends Thread {
    private final Process cassandra;
    private final String cassandraDesc;

    CassandraKiller(Process cassandra, String cassandraDesc) {
        this.cassandra = cassandra;
        this.cassandraDesc = cassandraDesc;
    }

    @Override
    public void run() {
        System.err.println("Terminating Cassandra " + cassandraDesc + "...");
        cassandra.destroy();
    }
}
