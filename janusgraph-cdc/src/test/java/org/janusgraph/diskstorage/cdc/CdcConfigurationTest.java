// Copyright 2025 JanusGraph Authors
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

package org.janusgraph.diskstorage.cdc;

import org.janusgraph.diskstorage.configuration.BasicConfiguration;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.diskstorage.configuration.backend.CommonsConfiguration;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for CDC configuration and transaction wrapping.
 */
public class CdcConfigurationTest {

    @Test
    public void testCdcConfigurationDisabled() {
        WriteConfiguration config = new CommonsConfiguration();
        config.set("index.search.backend", "elasticsearch");
        
        BasicConfiguration basicConfig = new BasicConfiguration(
            GraphDatabaseConfiguration.ROOT_NS,
            config,
            BasicConfiguration.Restriction.NONE
        );
        
        CdcConfiguration cdcConfig = new CdcConfiguration(basicConfig.restrictTo("search"));
        assertFalse(cdcConfig.isEnabled());
    }

    @Test
    public void testCdcConfigurationEnabled() {
        WriteConfiguration config = new CommonsConfiguration();
        config.set("index.search.cdc.enabled", true);
        config.set("index.search.cdc.kafka-bootstrap-servers", "localhost:9092");
        config.set("index.search.cdc.kafka-topic", "test-topic");
        config.set("index.search.cdc.mode", "dual");
        
        BasicConfiguration basicConfig = new BasicConfiguration(
            GraphDatabaseConfiguration.ROOT_NS,
            config,
            BasicConfiguration.Restriction.NONE
        );
        
        CdcConfiguration cdcConfig = new CdcConfiguration(basicConfig.restrictTo("search"));
        assertTrue(cdcConfig.isEnabled());
        assertEquals("localhost:9092", cdcConfig.getKafkaBootstrapServers());
        assertEquals("test-topic", cdcConfig.getKafkaTopic());
        assertEquals(CdcIndexTransaction.CdcMode.DUAL, cdcConfig.getMode());
    }

    @Test
    public void testCdcModeDefaults() {
        WriteConfiguration config = new CommonsConfiguration();
        config.set("index.search.cdc.enabled", true);
        config.set("index.search.cdc.kafka-bootstrap-servers", "localhost:9092");
        config.set("index.search.cdc.kafka-topic", "test-topic");
        
        BasicConfiguration basicConfig = new BasicConfiguration(
            GraphDatabaseConfiguration.ROOT_NS,
            config,
            BasicConfiguration.Restriction.NONE
        );
        
        CdcConfiguration cdcConfig = new CdcConfiguration(basicConfig.restrictTo("search"));
        assertEquals(CdcIndexTransaction.CdcMode.DUAL, cdcConfig.getMode());
    }
}
