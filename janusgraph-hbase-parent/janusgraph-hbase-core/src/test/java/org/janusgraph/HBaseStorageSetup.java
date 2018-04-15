// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph;

import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.diskstorage.hbase.HBaseStoreManager;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.database.idassigner.placement.SimpleBulkPlacementStrategy;

import org.apache.commons.lang3.StringUtils;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.util.VersionInfo;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HBaseStorageSetup {

    private static final Logger log = LoggerFactory.getLogger(HBaseStorageSetup.class);

    // hbase config for testing

    public static final String HBASE_PARENT_DIR_PROP = "test.hbase.parentdir";

    private static final Pattern HBASE_SUPPORTED_VERSION_PATTERN = Pattern.compile("^((0\\.9[8])|(1\\.[0123]))\\..*");

    private static final String HBASE_VERSION_1_STRING = "1.";

    private static final String HBASE_PARENT_DIR;

    private static final String HBASE_TARGET_VERSION = VersionInfo.getVersion();

    static {
        String parentDir = "..";
        String tmp = System.getProperty(HBASE_PARENT_DIR_PROP);
        if (null != tmp) {
            parentDir = tmp;
        }
        HBASE_PARENT_DIR = parentDir;
    }

    private static final String HBASE_STAT_FILE = "/tmp/janusgraph-hbase-test-daemon.stat";

    private volatile static HBaseStatus HBASE = null;

    public static String getScriptDirForHBaseVersion(String hv) {
        return getDirForHBaseVersion(hv, "bin");
    }

    public static String getConfDirForHBaseVersion(String hv) {
        return getDirForHBaseVersion(hv, "conf");
    }

    public static String getDirForHBaseVersion(String hv, String lastSubdir) {
        Matcher m = HBASE_SUPPORTED_VERSION_PATTERN.matcher(hv);
        if (m.matches()) {
            String majorDotMinor = m.group(1);
            if (majorDotMinor.startsWith(HBASE_VERSION_1_STRING)) {
                // All HBase 1.x maps to 10
                majorDotMinor = "1.0";
            }
            String result = String.format("%s%sjanusgraph-hbase-%s/%s/", HBASE_PARENT_DIR, File.separator, majorDotMinor.replace(".", ""), lastSubdir);
            log.debug("Built {} path for HBase version {}: {}", lastSubdir, hv, result);
            return result;
        } else {
            throw new RuntimeException("Unsupported HBase test version " + hv + " does not match pattern " + HBASE_SUPPORTED_VERSION_PATTERN);
        }
    }

    public static ModifiableConfiguration getHBaseConfiguration() {
        return getHBaseConfiguration("");
    }

    public static ModifiableConfiguration getHBaseConfiguration(String tableName) {
        return getHBaseConfiguration(tableName, "");
    }

    public static ModifiableConfiguration getHBaseConfiguration(String tableName, String graphName) {
        ModifiableConfiguration config = GraphDatabaseConfiguration.buildGraphConfiguration();
        config.set(GraphDatabaseConfiguration.STORAGE_BACKEND, "hbase");
        if (!StringUtils.isEmpty(tableName)) config.set(HBaseStoreManager.HBASE_TABLE, tableName);
        if (!StringUtils.isEmpty(graphName)) config.set(GraphDatabaseConfiguration.GRAPH_NAME, graphName);
        config.set(GraphDatabaseConfiguration.TIMESTAMP_PROVIDER, HBaseStoreManager.PREFERRED_TIMESTAMPS);
        config.set(SimpleBulkPlacementStrategy.CONCURRENT_PARTITIONS, 1);
        config.set(GraphDatabaseConfiguration.DROP_ON_CLEAR, false);

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
    public synchronized static HBaseStatus startHBase() throws IOException {
        if (HBASE != null) {
            log.info("HBase already started");
            return HBASE;
        }

        killIfRunning();

        deleteData();

        log.info("Starting HBase");
        String scriptPath = getScriptDirForHBaseVersion(HBASE_TARGET_VERSION) + "/hbase-daemon.sh";
        DaemonRunner.runCommand(scriptPath, "--config", getConfDirForHBaseVersion(HBASE_TARGET_VERSION), "start", "master");

        HBASE = HBaseStatus.write(HBASE_STAT_FILE, HBASE_TARGET_VERSION);

        registerKillerHook(HBASE);

        waitForConnection(60L, TimeUnit.SECONDS);

        return HBASE;
    }

    /**
     * Check whether {@link #HBASE_STAT_FILE} describes an HBase daemon. If so,
     * kill it. Otherwise, do nothing.
     */
    public synchronized static void killIfRunning() {
        HBaseStatus stat = HBaseStatus.read(HBASE_STAT_FILE);

        if (null == stat) {
            log.info("HBase is not running");
            return;
        }

        shutdownHBase(stat);
    }

    public synchronized static void waitForConnection(long timeout, TimeUnit timeoutUnit) {
        long before = System.currentTimeMillis();
        long after;
        long timeoutMS = TimeUnit.MILLISECONDS.convert(timeout, timeoutUnit);
        do {
            try {
                HConnection hc = HConnectionManager.createConnection(HBaseConfiguration.create());
                hc.close();
                after = System.currentTimeMillis();
                log.info("HBase server to started after about {} ms", after - before);
                return;
            } catch (IOException e) {
                log.info("Exception caught while waiting for the HBase server to start", e);
            }
            after = System.currentTimeMillis();
        } while (timeoutMS > after - before);
        after = System.currentTimeMillis();
        log.warn("HBase server did not start in {} ms", after - before);
    }

    /**
     * Delete HBase data under the current working directory.
     */
    private synchronized static void deleteData() {
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
    private synchronized static void shutdownHBase(HBaseStatus stat) {

        log.info("Shutting down HBase...");

        // First try graceful shutdown through the script...
        DaemonRunner.runCommand(stat.getScriptDir() + "/hbase-daemon.sh", "--config", stat.getConfDir(), "stop", "master");

        log.info("Shutdown HBase");

        stat.getFile().delete();

        log.info("Deleted {}", stat.getFile());

        HBASE = null;
    }
}
