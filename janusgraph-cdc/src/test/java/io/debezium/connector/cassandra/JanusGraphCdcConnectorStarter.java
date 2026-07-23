// Copyright 2026 JanusGraph Authors
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

package io.debezium.connector.cassandra;

import io.debezium.config.Configuration;

/**
 * Test-only bridge that starts the Debezium Cassandra connector embedded in the test JVM. Lives in Debezium's package
 * because {@link CassandraConnectorTask#init(CassandraConnectorConfig, ComponentFactory)} is package-private.
 */
public final class JanusGraphCdcConnectorStarter {

    private JanusGraphCdcConnectorStarter() {
    }

    /** Builds, initializes and starts a standalone (embedded, Kafka-producing) Cassandra connector. */
    public static CassandraConnectorTaskTemplate startEmbedded(Configuration configuration) throws Exception {
        CassandraConnectorTaskTemplate template =
            CassandraConnectorTask.init(new CassandraConnectorConfig(configuration), new ComponentFactoryStandalone());
        template.start();
        return template;
    }
}
