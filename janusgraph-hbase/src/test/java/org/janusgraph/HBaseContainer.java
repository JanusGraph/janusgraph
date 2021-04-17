// Copyright 2021 JanusGraph Authors
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

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.janusgraph.core.JanusGraphException;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.TemporaryBackendException;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.diskstorage.hbase.AdminMask;
import org.janusgraph.diskstorage.hbase.ConnectionMask;
import org.janusgraph.diskstorage.hbase.HBaseCompat;
import org.janusgraph.diskstorage.hbase.HBaseCompatLoader;
import org.janusgraph.diskstorage.hbase.HBaseStoreManager;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.database.idassigner.placement.SimpleBulkPlacementStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.ImageFromDockerfile;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class HBaseContainer extends GenericContainer<HBaseContainer> {
    private static final Logger logger = LoggerFactory.getLogger(HBaseContainer.class);

    public static final String HBASE_TARGET_DIR = "test.hbase.targetdir";
    private static final String DEFAULT_VERSION = "2.2.7";
    private static final String DEFAULT_UID = "1000";
    private static final String DEFAULT_GID = "1000";

    private final HBaseCompat compat;

    private static String getVersion() {
        String property = System.getProperty("hbase.docker.version");
        if (StringUtils.isNotEmpty(property))
            return property;
        return DEFAULT_VERSION;
    }

    private static String getUid() {
        String property = System.getProperty("hbase.docker.uid");
        if (StringUtils.isNotEmpty(property))
            return property;
        return DEFAULT_UID;
    }

    private static String getGid() {
        String property = System.getProperty("hbase.docker.gid");
        if (StringUtils.isNotEmpty(property))
            return property;
        return DEFAULT_GID;
    }

    private static String getTargetDir() {
        String property = System.getProperty(HBASE_TARGET_DIR);
        if (StringUtils.isNotEmpty(property))
            return property;
        return Paths.get(System.getProperty("user.dir"), "target").toString();
    }

    public HBaseContainer() {
        this(false);
    }

    public HBaseContainer(boolean mountRoot) {
        super(new ImageFromDockerfile()
            .withFileFromPath(".", Paths.get("docker"))
            .withBuildArg("HBASE_VERSION", getVersion())
            .withBuildArg("HBASE_UID", getUid())
            .withBuildArg("HBASE_GID", getGid()));
        addFixedExposedPort(2181, 2181);
        addFixedExposedPort(16000, 16000);
        addFixedExposedPort(16010, 16010);
        addFixedExposedPort(16020, 16020);
        addFixedExposedPort(16030, 16030);

        if (mountRoot){
            try {
                Files.createDirectories(getHBaseRootdir());
            } catch (IOException e) {
                logger.warn("failed to create folder", e);
                throw new JanusGraphException(e);
            }
            addFileSystemBind(getHBaseRootdir().toString(), "/data/hbase", BindMode.READ_WRITE);
        }

        withCreateContainerCmdModifier(createContainerCmd -> {
            createContainerCmd
                .withHostName("localhost");
        });
        waitingFor(Wait.forLogMessage(".*Master has completed initialization.*", 1));
        compat = HBaseCompatLoader.getCompat(null);
    }

    public Path getHBaseRootdir() {
        return Paths.get(getTargetDir(), "hbase-root");
    }

    private ConnectionMask createConnectionMask() throws IOException {
        Configuration entries = HBaseConfiguration.create();
        entries.set("hbase.zookeeper.quorum", "localhost");
        return compat.createConnection(entries);
    }

    /**
     * Create a snapshot for a table.
     *
     * @param snapshotName
     * @param table
     * @throws BackendException
     */
    public synchronized void createSnapshot(String snapshotName, String table)
        throws BackendException {
        try (ConnectionMask hc = createConnectionMask();
             AdminMask admin = hc.getAdmin()) {
            admin.snapshot(snapshotName, table);
        } catch (Exception e) {
            logger.warn("Create HBase snapshot failed", e);
            throw new TemporaryBackendException("Create HBase snapshot failed", e);
        }
    }

    /**
     * Delete a snapshot.
     *
     * @param snapshotName
     * @throws IOException
     */
    public synchronized void deleteSnapshot(String snapshotName) throws IOException {
        try (ConnectionMask hc = createConnectionMask();
             AdminMask admin = hc.getAdmin()) {
            admin.deleteSnapshot(snapshotName);
        }
    }

    public WriteConfiguration getWriteConfiguration() {
        return getModifiableConfiguration().getConfiguration();
    }

    public ModifiableConfiguration getModifiableConfiguration() {
        return getNamedConfiguration(null, null);
    }

    public ModifiableConfiguration getNamedConfiguration(String tableName, String graphName) {
        ModifiableConfiguration config = GraphDatabaseConfiguration.buildGraphConfiguration();
        config.set(GraphDatabaseConfiguration.STORAGE_BACKEND, "hbase");
        if (StringUtils.isNotEmpty(tableName)) config.set(HBaseStoreManager.HBASE_TABLE, tableName);
        if (StringUtils.isNotEmpty(graphName)) config.set(GraphDatabaseConfiguration.GRAPH_NAME, graphName);
        config.set(GraphDatabaseConfiguration.TIMESTAMP_PROVIDER, HBaseStoreManager.PREFERRED_TIMESTAMPS);
        config.set(GraphDatabaseConfiguration.STORAGE_HOSTS, new String[]{"localhost"});
        config.set(SimpleBulkPlacementStrategy.CONCURRENT_PARTITIONS, 1);
        config.set(GraphDatabaseConfiguration.DROP_ON_CLEAR, false);
        return config;
    }
}
