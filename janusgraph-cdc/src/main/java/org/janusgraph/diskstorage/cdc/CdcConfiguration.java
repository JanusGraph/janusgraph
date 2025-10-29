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

import org.janusgraph.diskstorage.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.INDEX_CDC_ENABLED;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.INDEX_CDC_KAFKA_BOOTSTRAP_SERVERS;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.INDEX_CDC_KAFKA_TOPIC;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.INDEX_CDC_MODE;

/**
 * Configuration holder for CDC settings.
 */
public class CdcConfiguration {

    private static final Logger log = LoggerFactory.getLogger(CdcConfiguration.class);

    private final boolean enabled;
    private final CdcIndexTransaction.CdcMode mode;
    private final String kafkaBootstrapServers;
    private final String kafkaTopic;

    public CdcConfiguration(Configuration config) {
        this.enabled = config.get(INDEX_CDC_ENABLED);
        this.mode = parseCdcMode(config.get(INDEX_CDC_MODE));
        this.kafkaBootstrapServers = config.get(INDEX_CDC_KAFKA_BOOTSTRAP_SERVERS);
        this.kafkaTopic = config.get(INDEX_CDC_KAFKA_TOPIC);

        if (enabled) {
            validate();
            log.info("CDC enabled with mode: {}, topic: {}, bootstrap servers: {}", 
                    mode, kafkaTopic, kafkaBootstrapServers);
        }
    }

    private CdcIndexTransaction.CdcMode parseCdcMode(String modeStr) {
        if (modeStr == null || modeStr.isEmpty()) {
            return CdcIndexTransaction.CdcMode.DUAL;
        }

        switch (modeStr.toLowerCase()) {
            case "skip":
                return CdcIndexTransaction.CdcMode.SKIP;
            case "dual":
                return CdcIndexTransaction.CdcMode.DUAL;
            case "cdc-only":
            case "cdc_only":
                return CdcIndexTransaction.CdcMode.CDC_ONLY;
            default:
                log.warn("Unknown CDC mode: {}, defaulting to DUAL", modeStr);
                return CdcIndexTransaction.CdcMode.DUAL;
        }
    }

    private void validate() {
        if (kafkaBootstrapServers == null || kafkaBootstrapServers.isEmpty()) {
            throw new IllegalArgumentException("CDC is enabled but kafka bootstrap servers are not configured");
        }
        if (kafkaTopic == null || kafkaTopic.isEmpty()) {
            throw new IllegalArgumentException("CDC is enabled but kafka topic is not configured");
        }
    }

    public boolean isEnabled() {
        return enabled;
    }

    public CdcIndexTransaction.CdcMode getMode() {
        return mode;
    }

    public String getKafkaBootstrapServers() {
        return kafkaBootstrapServers;
    }

    public String getKafkaTopic() {
        return kafkaTopic;
    }
}
