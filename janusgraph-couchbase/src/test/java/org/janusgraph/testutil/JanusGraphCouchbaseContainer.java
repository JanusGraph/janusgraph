/*
 * Copyright 2025 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.janusgraph.testutil;

import org.janusgraph.diskstorage.configuration.BasicConfiguration;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.diskstorage.configuration.backend.CommonsConfiguration;
import org.janusgraph.diskstorage.couchbase.CouchbaseConfigOptions;
import org.janusgraph.diskstorage.couchbase.CouchbaseIndex;
import org.janusgraph.diskstorage.couchbase.CouchbaseIndexConfigOptions;
import org.janusgraph.diskstorage.couchbase.CouchbaseStoreManager;
import org.janusgraph.graphdb.JanusGraphIndexTest;
import org.janusgraph.util.system.ConfigurationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.couchbase.BucketDefinition;
import org.testcontainers.couchbase.CouchbaseContainer;
import org.testcontainers.couchbase.CouchbaseService;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.INDEX_BACKEND;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.ROOT_NS;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_BACKEND;

public class JanusGraphCouchbaseContainer extends CouchbaseContainer {
    private static final Logger log = LoggerFactory.getLogger(JanusGraphCouchbaseContainer.class);
    private static final Integer DEFAULT_DELAY = 0;
    private boolean useServer = true;
    private boolean secondIndexEnabled;
    private Integer clusterDelay = DEFAULT_DELAY;

    public JanusGraphCouchbaseContainer() {
        super("couchbase/server:enterprise-7.2.0");
        withStartupTimeout(Duration.ofSeconds(30));
        withBucket(new BucketDefinition("janusgraph-couchbase-bucket"));
//        withBucket(new BucketDefinition("search2"));
        withEnabledServices(CouchbaseService.KV, CouchbaseService.INDEX, CouchbaseService.QUERY, CouchbaseService.SEARCH);
        withStartupTimeout(Duration.of(1, ChronoUnit.MINUTES));
        withStartupAttempts(3);
        withCredentials("Administrator", "password");
    }

    @Override
    public void stop() {
        log.info("Shutting down couchbase server");
        super.stop();
    }

    @Override
    public void start() {
        if (!useServer) {
            log.info("Starting Couchbase server");
            super.start();
        }
    }

    public WriteConfiguration getJanusGraphConfig() {
        ModifiableConfiguration config = new ModifiableConfiguration(ROOT_NS,
            new CommonsConfiguration(
                ConfigurationUtil.createBaseConfiguration()
            ),
            BasicConfiguration.Restriction.NONE)
            .set(STORAGE_BACKEND, CouchbaseStoreManager.class.getName())
            .set(INDEX_BACKEND, CouchbaseIndex.class.getName(), JanusGraphIndexTest.INDEX);
        if (secondIndexEnabled) {
            config.set(INDEX_BACKEND, CouchbaseIndex.class.getName(), JanusGraphIndexTest.INDEX2);
        }
        if (useServer) {
            config.set(CouchbaseConfigOptions.CLUSTER_CONNECT_STRING, "localhost")
                .set(CouchbaseConfigOptions.CLUSTER_CONNECT_USERNAME, "Administrator")
                .set(CouchbaseConfigOptions.CLUSTER_CONNECT_PASSWORD, "password")
                .set(CouchbaseIndexConfigOptions.CLUSTER_CONNECT_STRING, "localhost", JanusGraphIndexTest.INDEX)
                .set(CouchbaseIndexConfigOptions.CLUSTER_CONNECT_USERNAME, "Administrator", JanusGraphIndexTest.INDEX)
                .set(CouchbaseIndexConfigOptions.CLUSTER_CONNECT_PASSWORD, "password", JanusGraphIndexTest.INDEX);
            if (secondIndexEnabled) {
                config.set(CouchbaseIndexConfigOptions.CLUSTER_CONNECT_STRING, "localhost", JanusGraphIndexTest.INDEX2)
                    .set(CouchbaseIndexConfigOptions.CLUSTER_CONNECT_USERNAME, "Administrator", JanusGraphIndexTest.INDEX2)
                    .set(CouchbaseIndexConfigOptions.CLUSTER_CONNECT_PASSWORD, "password", JanusGraphIndexTest.INDEX2);
            }
        } else {
            config.set(CouchbaseConfigOptions.CLUSTER_CONNECT_STRING, getConnectionString())
                .set(CouchbaseConfigOptions.CLUSTER_CONNECT_USERNAME, getUsername())
                .set(CouchbaseConfigOptions.CLUSTER_CONNECT_PASSWORD, getPassword())
                .set(CouchbaseIndexConfigOptions.CLUSTER_CONNECT_STRING, getConnectionString(), JanusGraphIndexTest.INDEX)
                .set(CouchbaseIndexConfigOptions.CLUSTER_CONNECT_USERNAME, getUsername(), JanusGraphIndexTest.INDEX)
                .set(CouchbaseIndexConfigOptions.CLUSTER_CONNECT_PASSWORD, getPassword(), JanusGraphIndexTest.INDEX);
            if  (secondIndexEnabled) {
                config.set(CouchbaseIndexConfigOptions.CLUSTER_CONNECT_STRING, getConnectionString(), JanusGraphIndexTest.INDEX2)
                    .set(CouchbaseIndexConfigOptions.CLUSTER_CONNECT_USERNAME, getUsername(), JanusGraphIndexTest.INDEX2)
                    .set(CouchbaseIndexConfigOptions.CLUSTER_CONNECT_PASSWORD, getPassword(), JanusGraphIndexTest.INDEX2);
            }
        }
        config.set(CouchbaseConfigOptions.CLUSTER_CONNECT_BUCKET, "janusgraph-couchbase-bucket")
            .set(CouchbaseConfigOptions.CLUSTER_DEFAULT_SCOPE, "_default")
            .set(CouchbaseIndexConfigOptions.CLUSTER_CONNECT_BUCKET, "janusgraph-couchbase-bucket", JanusGraphIndexTest.INDEX)
            .set(CouchbaseIndexConfigOptions.CLUSTER_DEFAULT_SCOPE, "search", JanusGraphIndexTest.INDEX)
            .set(CouchbaseIndexConfigOptions.CLUSTER_TEST_DELAY_MS, clusterDelay, JanusGraphIndexTest.INDEX);
        if (secondIndexEnabled) {
            config.set(CouchbaseIndexConfigOptions.CLUSTER_CONNECT_BUCKET, "janusgraph-couchbase-bucket", JanusGraphIndexTest.INDEX2)
                .set(CouchbaseIndexConfigOptions.CLUSTER_DEFAULT_SCOPE, "search2", JanusGraphIndexTest.INDEX2)
                .set(CouchbaseIndexConfigOptions.CLUSTER_TEST_DELAY_MS, clusterDelay, JanusGraphIndexTest.INDEX2);
        }


        return config.getConfiguration();
    }

    public void enableIndex2() {
        secondIndexEnabled = true;
    }

    public void disableIndex2() {
        secondIndexEnabled = false;
    }

    public void setClusterDelay(Integer delay) {
        this.clusterDelay = delay;
    }

    public void resetClusterDelay() {
        this.clusterDelay = DEFAULT_DELAY;
    }
}
