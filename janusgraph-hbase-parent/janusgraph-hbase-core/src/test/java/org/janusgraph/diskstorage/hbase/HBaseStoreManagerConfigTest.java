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

package org.janusgraph.diskstorage.hbase;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.io.IOException;
import java.io.StringWriter;

import org.apache.log4j.Appender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.WriterAppender;
import org.janusgraph.HBaseStorageSetup;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.configuration.BasicConfiguration;
import org.janusgraph.diskstorage.configuration.ConfigElement;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStore;
import org.janusgraph.diskstorage.util.time.TimestampProviders;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.configuration.builder.GraphDatabaseConfigurationBuilder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class HBaseStoreManagerConfigTest {

    @BeforeAll
    public static void startHBase() throws IOException, BackendException {
        HBaseStorageSetup.startHBase();
    }

    @AfterAll
    public static void stopHBase() {
        // No op. HBase stopped by shutdown hook registered by startHBase().

    }

    @Test
    public void testShortCfNames() throws Exception {
        Logger log = Logger.getLogger(HBaseStoreManager.class);
        Level savedLevel = log.getLevel();
        log.setLevel(Level.WARN);
        StringWriter writer = new StringWriter();
        Appender appender = new WriterAppender(new PatternLayout("%p: %m%n"), writer);
        log.addAppender(appender);

        // Open the HBaseStoreManager and store with default SHORT_CF_NAMES true.
        WriteConfiguration config = HBaseStorageSetup.getHBaseGraphConfiguration();
        HBaseStoreManager manager = new HBaseStoreManager(new BasicConfiguration(GraphDatabaseConfiguration.ROOT_NS,
            config, BasicConfiguration.Restriction.NONE));
        KeyColumnValueStore store = manager.openDatabase(GraphDatabaseConfiguration.SYSTEM_PROPERTIES_STORE_NAME);

        store.close();
        manager.close();

        // Open the HBaseStoreManager and store with SHORT_CF_NAMES false.
        config.set(ConfigElement.getPath(HBaseStoreManager.SHORT_CF_NAMES), false);
        manager = new HBaseStoreManager(new BasicConfiguration(GraphDatabaseConfiguration.ROOT_NS,
            config, BasicConfiguration.Restriction.NONE));
        writer.getBuffer().setLength(0);
        store = manager.openDatabase(GraphDatabaseConfiguration.SYSTEM_PROPERTIES_STORE_NAME);

        // Verify we get WARN.
        assertTrue(writer.toString().startsWith("WARN: Configuration"), writer.toString());
        log.removeAppender(appender);
        log.setLevel(savedLevel);

        store.close();
        manager.close();
    }
    
    @Test
    // Test HBase preferred timestamp provider MILLI is set by default
    public void testHBaseTimestampProvider() throws BackendException {
        // Get an empty configuration
        // GraphDatabaseConfiguration.buildGraphConfiguration() only build an empty one.
        ModifiableConfiguration config = GraphDatabaseConfiguration.buildGraphConfiguration();
        // Set backend to hbase
        config.set(GraphDatabaseConfiguration.STORAGE_BACKEND, "hbase");
        // Instantiate a GraphDatabaseConfiguration based on the above
        GraphDatabaseConfiguration graphConfig = new GraphDatabaseConfigurationBuilder().build(config.getConfiguration());
        // Check the TIMESTAMP_PROVIDER has been set to the hbase preferred MILLI
        TimestampProviders provider = graphConfig.getConfiguration().get(GraphDatabaseConfiguration.TIMESTAMP_PROVIDER);
        assertEquals(HBaseStoreManager.PREFERRED_TIMESTAMPS, provider);
    }

    @Test
    public void testHBaseStoragePort() throws BackendException {
        WriteConfiguration config = HBaseStorageSetup.getHBaseGraphConfiguration();
        config.set(ConfigElement.getPath(GraphDatabaseConfiguration.STORAGE_PORT), 2000);
        HBaseStoreManager manager = new HBaseStoreManager(new BasicConfiguration(GraphDatabaseConfiguration.ROOT_NS,
                    config, BasicConfiguration.Restriction.NONE));
        // Check the native property in HBase conf.
        String port = manager.getHBaseConf().get("hbase.zookeeper.property.clientPort");
        assertEquals("2000", port);
    }
}
