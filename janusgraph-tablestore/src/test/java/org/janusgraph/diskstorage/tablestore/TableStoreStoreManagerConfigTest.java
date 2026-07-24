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

package org.janusgraph.diskstorage.tablestore;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.appender.WriterAppender;
import org.apache.logging.log4j.core.filter.LevelMatchFilter;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.janusgraph.TableStoreContainer;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.PermanentBackendException;
import org.janusgraph.diskstorage.common.DistributedStoreManager;
import org.janusgraph.diskstorage.configuration.BasicConfiguration;
import org.janusgraph.diskstorage.configuration.ConfigElement;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStore;
import org.janusgraph.diskstorage.util.time.TimestampProviders;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.configuration.builder.GraphDatabaseConfigurationBuilder;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.StringWriter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
public class TableStoreStoreManagerConfigTest {
    @Container
    public static final TableStoreContainer tableStoreContainer = new TableStoreContainer();

    @Test
    public void testShortCfNames() throws Exception {
        org.apache.logging.log4j.core.Logger log = (org.apache.logging.log4j.core.Logger)LogManager.getLogger(TableStoreStoreManager.class);
        StringWriter writer = new StringWriter();
        Appender appender = WriterAppender.createAppender(PatternLayout.newBuilder().withPattern("%p: %m%n").build(), LevelMatchFilter.newBuilder().setLevel(Level.WARN).build(), writer, "test", false, false);
        appender.start();
        log.addAppender(appender);

        // Open the TableStoreStoreManager and store with default SHORT_CF_NAMES true.
        WriteConfiguration config = tableStoreContainer.getWriteConfiguration();
        TableStoreStoreManager manager = new TableStoreStoreManager(new BasicConfiguration(GraphDatabaseConfiguration.ROOT_NS,
            config, BasicConfiguration.Restriction.NONE));
        KeyColumnValueStore store = manager.openDatabase(GraphDatabaseConfiguration.SYSTEM_PROPERTIES_STORE_NAME);

        store.close();
        manager.close();

        // Open the TableStoreStoreManager and store with SHORT_CF_NAMES false.
        config.set(ConfigElement.getPath(TableStoreStoreManager.SHORT_CF_NAMES), false);
        manager = new TableStoreStoreManager(new BasicConfiguration(GraphDatabaseConfiguration.ROOT_NS,
            config, BasicConfiguration.Restriction.NONE));
        writer.getBuffer().setLength(0);
        store = manager.openDatabase(GraphDatabaseConfiguration.SYSTEM_PROPERTIES_STORE_NAME);

        // Verify we get WARN.
        assertTrue(writer.toString().startsWith("WARN: Configuration"), writer.toString());
        log.removeAppender(appender);

        store.close();
        manager.close();
    }

    @Test
    // Test TableStore preferred timestamp provider MILLI is set by default
    public void testTableStoreTimestampProvider() {
        // Get an empty configuration
        // GraphDatabaseConfiguration.buildGraphConfiguration() only build an empty one.
        ModifiableConfiguration config = GraphDatabaseConfiguration.buildGraphConfiguration();
        // Set backend to tableStore
        config.set(GraphDatabaseConfiguration.STORAGE_BACKEND, "tableStore");
        // Instantiate a GraphDatabaseConfiguration based on the above
        GraphDatabaseConfiguration graphConfig = new GraphDatabaseConfigurationBuilder().build(config.getConfiguration());
        // Check the TIMESTAMP_PROVIDER has been set to the TableStore preferred MILLI
        TimestampProviders provider = graphConfig.getConfiguration().get(GraphDatabaseConfiguration.TIMESTAMP_PROVIDER);
        assertEquals(TableStoreStoreManager.PREFERRED_TIMESTAMPS, provider);
    }

    @Test
    public void testTableStoreStoragePort() throws BackendException {
        WriteConfiguration config = tableStoreContainer.getWriteConfiguration();
        config.set(ConfigElement.getPath(GraphDatabaseConfiguration.STORAGE_PORT), 2000);
        TableStoreStoreManager manager = new TableStoreStoreManager(new BasicConfiguration(GraphDatabaseConfiguration.ROOT_NS,
                    config, BasicConfiguration.Restriction.NONE));
        // Check the native property in TableStore conf.
        String port = manager.getHBaseConf().get("hbase.zookeeper.property.clientPort");
        assertEquals("2000", port);
    }

    @Test
    // Test TableStore skip-schema-check config
    public void testTableStoreSkipSchemaCheck() throws Exception {
        org.apache.logging.log4j.core.Logger log = (org.apache.logging.log4j.core.Logger)LogManager.getLogger(TableStoreStoreManager.class);
        Level savedLevel = log.getLevel();
        log.setLevel(Level.DEBUG);
        StringWriter writer = new StringWriter();
        Appender appender = WriterAppender.createAppender(PatternLayout.newBuilder().withPattern("%p: %m%n").build(), LevelMatchFilter.newBuilder().setLevel(Level.DEBUG).build(), writer, "test", false, false);
        appender.start();
        log.addAppender(appender);

        // Open the TableStoreStoreManager with default skip-schema-check false.
        WriteConfiguration config = tableStoreContainer.getWriteConfiguration();
        TableStoreStoreManager manager = new TableStoreStoreManager(new BasicConfiguration(GraphDatabaseConfiguration.ROOT_NS,
                                                                                 config, BasicConfiguration.Restriction.NONE));
        assertEquals(manager.getDeployment(), DistributedStoreManager.Deployment.REMOTE);
        // Verify we get "Performing schema check".
        assertTrue(writer.toString().contains("Performing schema check"), writer.toString());
        manager.close();

        // Open the TableStoreStoreManager with skip-schema-check true.
        config.set(ConfigElement.getPath(TableStoreStoreManager.SKIP_SCHEMA_CHECK), true);
        manager = new TableStoreStoreManager(new BasicConfiguration(GraphDatabaseConfiguration.ROOT_NS,
                                                               config, BasicConfiguration.Restriction.NONE));
        writer.getBuffer().setLength(0);

        assertEquals(manager.getDeployment(), DistributedStoreManager.Deployment.REMOTE);
        // Verify we get "Skipping schema check".
        assertTrue(writer.toString().contains("Skipping schema check"), writer.toString());

        log.removeAppender(appender);
        log.setLevel(savedLevel);
        // Test when tableStore table does not exist with skip-schema-check true.
        config.set(ConfigElement.getPath(TableStoreStoreManager.HBASE_TABLE), "unknown_table");
        TableStoreStoreManager skipSchemaManager = new TableStoreStoreManager(new BasicConfiguration(GraphDatabaseConfiguration.ROOT_NS,
                                                                                 config, BasicConfiguration.Restriction.NONE));
        Exception ex = assertThrows(PermanentBackendException.class, () -> skipSchemaManager.getLocalKeyPartition());
        assertEquals("Table unknown_table doesn't exist in TableStore!", ex.getMessage());

        manager.close();
    }
}
