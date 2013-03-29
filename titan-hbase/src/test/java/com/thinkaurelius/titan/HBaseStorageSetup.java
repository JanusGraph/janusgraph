package com.thinkaurelius.titan;

import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

public class HBaseStorageSetup {
    private static Process HBASE = null;
    // amount of seconds to wait before assuming that HBase shutdown
    private static final int SHUTDOWN_TIMEOUT_SEC = 20;

    // hbase config for testing
    private static final String HBASE_CONFIG_DIR = "./src/test/config";

    // default pid file location
    private static final String HBASE_PID_FILE = "/tmp/hbase-" + System.getProperty("user.name") + "-master.pid";

    static {
        try {
            System.out.println("Deleteing old test directories (if any).");

            // please keep in sync with HBASE_CONFIG_DIR/hbase-site.xml, reading HBase XML config is huge pain.
            File hbaseRoot = new File("./src/test/titan-hbase-test-data");
            File zookeeperDataDir = new File("./src/test/titan-zookeeper-test");

            if (hbaseRoot.exists())
                FileUtils.deleteDirectory(hbaseRoot);

            if (zookeeperDataDir.exists())
                FileUtils.deleteDirectory(zookeeperDataDir);
        } catch (IOException e) {
            System.err.println("Failed to delete old HBase test directories: '" + e.getMessage() + "', ignoring.");
        }

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                System.out.println("All done. Shutting done HBase.");

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
        return config;
    }

    public static Configuration getHBaseGraphConfiguration() {
        BaseConfiguration config = new BaseConfiguration();
        config.subset(GraphDatabaseConfiguration.STORAGE_NAMESPACE).addProperty(GraphDatabaseConfiguration.STORAGE_BACKEND_KEY, "hbase");
        return config;
    }

    public static void startHBase() throws IOException {
        if (HBASE != null)
            return; // already started, nothing to do

        try {
            // start HBase instance with environment set
            HBASE = Runtime.getRuntime().exec(String.format("./bin/hbase-daemon.sh --config %s start master", HBASE_CONFIG_DIR));

            try {
                HBASE.waitFor(); // wait for script to return
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            assert HBASE.exitValue() >= 0; // check if we have started successfully
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void shutdownHBase() throws IOException {
        if (HBASE == null)
            return; // HBase hasn't been started yet

        try {
            File pid = new File(HBASE_PID_FILE);

            if (pid.exists()) {
                RandomAccessFile pidFile = new RandomAccessFile(pid, "r");

                StringBuilder b = new StringBuilder();

                while (pidFile.getFilePointer() < (pidFile.length() - 1)) // we don't need newline
                   b.append((char) pidFile.readByte());

                Process kill = Runtime.getRuntime().exec("kill -TERM " + b.toString());
                kill.waitFor();

                pidFile.close();
                pid.delete(); // delete pid file like nothing happened

                return;
            }

            // fall back to scripting
            Runtime.getRuntime().exec(String.format("./bin/hbase-daemon.sh --config %s stop master", HBASE_CONFIG_DIR));

            System.out.println("Waiting 20 seconds for HBase to shutdown...");

            Thread.sleep(SHUTDOWN_TIMEOUT_SEC * 1000); // wait no longer than timeout seconds
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }
}
