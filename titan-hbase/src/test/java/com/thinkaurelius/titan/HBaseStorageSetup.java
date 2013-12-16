package com.thinkaurelius.titan;

import com.google.common.base.Joiner;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;

public class HBaseStorageSetup {
    
    private static Process HBASE = null;
    // amount of seconds to wait before assuming that HBase shutdown
    private static final int SHUTDOWN_TIMEOUT_SEC = 20;

    // hbase config for testing
    private static final String HBASE_CONFIG_DIR = "./conf";

    // default pid file location
    private static final String HBASE_PID_FILE = "/tmp/hbase-" + System.getProperty("user.name") + "-master.pid";
    
    private static final Logger log = LoggerFactory.getLogger(HBaseStorageSetup.class);

    static {
        try {
            log.info("Deleting old test directories (if any).");

            // please keep in sync with HBASE_CONFIG_DIR/hbase-site.xml, reading HBase XML config is huge pain.
            File hbaseRoot = new File("./target/hbase-root");
            File zookeeperDataDir = new File("./target/zk-data");

            if (hbaseRoot.exists())
                FileUtils.deleteDirectory(hbaseRoot);

            if (zookeeperDataDir.exists())
                FileUtils.deleteDirectory(zookeeperDataDir);
        } catch (IOException e) {
            log.error("Failed to delete old HBase test directories: '" + e.getMessage() + "', ignoring.");
        }

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                log.info("All done. Shutting done HBase.");

                try {
                    HBaseStorageSetup.shutdownHBase();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }

    public static Configuration getHBaseStorageConfiguration() {
        BaseConfiguration config = new BaseConfiguration();
        config.addProperty(GraphDatabaseConfiguration.STORAGE_BACKEND_KEY, "hbase");
        config.addProperty(GraphDatabaseConfiguration.CONNECTION_TIMEOUT_KEY, 60000L);
        return config;
    }

    public static Configuration getHBaseGraphConfiguration() {
        BaseConfiguration config = new BaseConfiguration();
        config.subset(GraphDatabaseConfiguration.STORAGE_NAMESPACE).addProperty(GraphDatabaseConfiguration.STORAGE_BACKEND_KEY, "hbase");
        config.subset(GraphDatabaseConfiguration.STORAGE_NAMESPACE).addProperty(GraphDatabaseConfiguration.CONNECTION_TIMEOUT_KEY, 60000L);
        return config;
    }

    public static void startHBase() throws IOException {
        if (HBASE != null) {
            log.info("HBase already started");
            return; // already started, nothing to do
        }

        try {
            log.info("Starting HBase");
            
            // start HBase instance with environment set
            String cmd = String.format("./bin/hbase-daemon.sh --config %s start master", HBASE_CONFIG_DIR);
            log.info("Executing {}", cmd);
            HBASE = Runtime.getRuntime().exec(cmd);

            try {
                HBASE.waitFor(); // wait for script to return
                log.info("HBase forked");
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            assert HBASE.exitValue() >= 0; // check if we have started successfully
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void shutdownHBase() throws IOException {
        if (HBASE == null)
            return; // HBase hasn't been started yet
        
        // First try graceful shutdown through the script...
        String cmdParts[] = new String[]{ "./bin/hbase-daemon.sh", "--config", HBASE_CONFIG_DIR, "stop", "master" };
        log.info("Executing {}", Joiner.on(" ").join(cmdParts));
        ProcessBuilder pb = new ProcessBuilder(cmdParts);
        pb.redirectErrorStream(true);
        Process stopMaster = pb.start();
        StreamLogger sl = new StreamLogger(stopMaster.getInputStream());
        sl.setDaemon(true);
        sl.start();
        
        // ...but send SIGKILL if that times out
        HBaseKiller killer = new HBaseKiller();
        killer.start();
        try {
            stopMaster.waitFor();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        killer.abort();
        killer.interrupt();
        try {
            killer.join();
        } catch (InterruptedException e) {
            log.warn("Failed to join HBase process killer thread", e);
        }
        
        // StreamLogger is a daemon thread, so failing to stop it isn't so bad
        try {
            sl.join(1000L);
        } catch (InterruptedException e) {
            log.warn("Failed to stop HBase output logger thread", e);
        }
    }
    
    private static class HBaseKiller extends Thread {
        
        private volatile boolean proceed = true;
        
        @Override
        public void run() {
            try {
                log.info("Waiting {} seconds for HBase to shutdown...", SHUTDOWN_TIMEOUT_SEC);
                Thread.sleep(SHUTDOWN_TIMEOUT_SEC * 1000L);
            } catch (InterruptedException e) {
                log.info("HBase killer thread interrupted");
            }
            
            if (proceed) {
                try {
                    readPidfileAndKill();
                } catch (Exception e) {
                    log.error("Failed to kill HBase", e);
                }
            } else {
                log.info("HBase shutdown cleanly, not attempting to kill its process");
            }
        }
        
        public void abort() {
            proceed = false;
        }
        
        private void readPidfileAndKill() throws IOException, InterruptedException {
            File pid = new File(HBASE_PID_FILE);

            if (pid.exists()) {
                RandomAccessFile pidFile = new RandomAccessFile(pid, "r");

                StringBuilder b = new StringBuilder();

                while (pidFile.getFilePointer() < (pidFile.length() - 1)) // we don't need newline
                   b.append((char) pidFile.readByte());

                String cmd = "kill -KILL " + b.toString();
                log.info("Executing {}", cmd);
                Process kill = Runtime.getRuntime().exec(cmd);
                kill.waitFor();

                pidFile.close();
                pid.delete(); // delete pid file like nothing happened

                return;
            }
        }
    }
    
    /*
     * This could be retired in favor of ProcessBuilder.Redirect when we move to
     * source level 1.7.
     */
    private static class StreamLogger extends Thread {
        
        private final BufferedReader reader;
        private static final Logger log =
                LoggerFactory.getLogger(StreamLogger.class);
        
        private StreamLogger(InputStream is) {
            this.reader = new BufferedReader(new InputStreamReader(is));
        }
        
        @Override
        public void run() {
            String line;
            try {
                while (null != (line = reader.readLine())) {
                    log.info("> {}", line);
                    if (Thread.currentThread().isInterrupted()) {
                        break;
                    }
                }
                    
                log.info("End of stream.");
            } catch (IOException e) {
                log.error("Unexpected IOException while reading stream {}", reader, e);
            }
        }
    }
}
