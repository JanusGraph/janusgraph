package com.thinkaurelius.titan;

import com.google.common.base.Joiner;
import com.thinkaurelius.titan.diskstorage.configuration.ModifiableConfiguration;
import com.thinkaurelius.titan.diskstorage.configuration.WriteConfiguration;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.database.idassigner.placement.SimpleBulkPlacementStrategy;

import org.apache.hadoop.hbase.util.VersionInfo;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.thinkaurelius.titan.HBaseStatus;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class HBaseStorageSetup {

    private static final Logger log = LoggerFactory.getLogger(HBaseStorageSetup.class);

    // hbase config for testing
    private static final String HBASE_CONFIG_DIR = "./conf";

    private static final String HBASE_PID_FILE = "/tmp/titan-hbase-test-daemon.pid";

    private static final String HBASE_TARGET_VERSION = VersionInfo.getVersion();

    private volatile static HBaseStatus HBASE = null;

    public static ModifiableConfiguration getHBaseConfiguration() {
        ModifiableConfiguration config = GraphDatabaseConfiguration.buildConfiguration();
        config.set(GraphDatabaseConfiguration.STORAGE_BACKEND, "hbase");
        config.set(GraphDatabaseConfiguration.CLUSTER_PARTITION, true);
        config.set(SimpleBulkPlacementStrategy.CONCURRENT_PARTITIONS, 1);
        return config;
    }

    public static WriteConfiguration getHBaseGraphConfiguration() {
        return getHBaseConfiguration().getConfiguration();
    }

    /**
     * Starts the HBase version described by {@link #HBASE_TARGET_VERSION}
     *
     * @return a status object describing a successfully-started HBase daemon
     * @throws IOException
     *             passed-through
     * @throws RuntimeException
     *             if starting HBase fails for any other reason
     */
    public static HBaseStatus startHBase() throws IOException {
        if (HBASE != null) {
            log.info("HBase already started");
            return HBASE;
        }

        killIfRunning();

        deleteData();

        log.info("Starting HBase");
        String scriptPath = HBaseStatus.getScriptDirForHBaseVersion(HBASE_TARGET_VERSION) + "/hbase-daemon.sh";
        runCommand(scriptPath, "--config", HBASE_CONFIG_DIR, "start", "master");

        HBASE = HBaseStatus.write(HBASE_PID_FILE, HBASE_TARGET_VERSION);

        registerKillerHook(HBASE);

        return HBASE;
    }

    /**
     * Check whether {@link #HBASE_PID_FILE} describes an HBase daemon. If so,
     * kill it. Otherwise, do nothing.
     */
    private static void killIfRunning() {
        HBaseStatus stat = HBaseStatus.read(HBASE_PID_FILE);

        if (null == stat) {
            log.info("HBase is not running");
            return;
        }

        shutdownHBase(stat);
    }

    /**
     * Delete HBase data under the current working directory.
     */
    private static void deleteData() {
        try {
            // please keep in sync with HBASE_CONFIG_DIR/hbase-site.xml, reading HBase XML config is huge pain.
            File hbaseRoot = new File("./target/hbase-root");
            File zookeeperDataDir = new File("./target/zk-data");

            if (hbaseRoot.exists()) {
                log.info("Deleting {}", hbaseRoot);
                FileUtils.deleteDirectory(hbaseRoot);
            }

            if (zookeeperDataDir.exists()) {
                log.info("Deleting {}", zookeeperDataDir);
                FileUtils.deleteDirectory(zookeeperDataDir);
            }

        } catch (IOException e) {
            throw new RuntimeException("Failed to delete old HBase test data directories", e);
        }
    }

    /**
     * Register a shutdown hook with the JVM that attempts to kill the external
     * HBase daemon
     *
     * @param stat
     *            the HBase daemon to kill
     */
    private static void registerKillerHook(final HBaseStatus stat) {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                shutdownHBase(stat);
            }
        });
    }

    /**
     * Runs the {@code hbase-daemon.sh stop master} script corresponding to the
     * HBase version described by the parameter.
     *
     * @param stat
     *            the running HBase daemon to stop
     */
    private static void shutdownHBase(HBaseStatus stat) {

        log.info("Shutting down HBase...");

        // First try graceful shutdown through the script...
        runCommand(stat.getScriptDir() + "/hbase-daemon.sh", "--config", HBASE_CONFIG_DIR, "stop", "master");

        log.info("Shutdown HBase");

        stat.getFile().delete();

        log.info("Deleted {}", stat.getFile());
    }

    /**
     * Run the parameter as an external process. Returns if the command starts
     * without throwing an exception and returns exit status 0. Throws an
     * exception if there's any problem invoking the command or if it does not
     * return zero exit status.
     *
     * Blocks indefinitely while waiting for the command to complete.
     *
     * @param argv
     *            passed directly to {@link ProcessBuilder}'s constructor
     */
    private static void runCommand(String... argv) {

        final String cmd = Joiner.on(" ").join(argv);
        log.info("Executing {}", cmd);

        ProcessBuilder pb = new ProcessBuilder(argv);
        pb.redirectErrorStream(true);
        Process startup;
        try {
            startup = pb.start();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        StreamLogger sl = new StreamLogger(startup.getInputStream());
        sl.setDaemon(true);
        sl.start();

        try {
            int exitcode = startup.waitFor(); // wait for script to return
            if (0 == exitcode) {
                log.info("Command \"{}\" exited with status 0", cmd);
            } else {
                throw new RuntimeException("Command \"" + cmd + "\" exited with status " + exitcode);
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        try {
            sl.join(1000L);
        } catch (InterruptedException e) {
            log.warn("Failed to cleanup stdin handler thread after running command \"{}\"", cmd, e);
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
