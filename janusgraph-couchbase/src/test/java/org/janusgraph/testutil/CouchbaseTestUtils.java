/*
 * Copyright 2023 Couchbase, Inc.
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

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Scope;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.couchbase.CouchbaseIndexConfigOptions;
import org.mockito.Mockito;
import org.testcontainers.couchbase.BucketDefinition;
import org.testcontainers.couchbase.CouchbaseContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.lang.ref.WeakReference;
import java.time.Duration;

import static com.couchbase.client.java.query.QueryOptions.queryOptions;
import static com.couchbase.client.java.query.QueryScanConsistency.REQUEST_PLUS;

public class CouchbaseTestUtils {

    public static final String BUCKET = "__bucket__";
    public static final String SCOPE = "__scope__";

    private static final BucketDefinition testBucketDefinition = new BucketDefinition("__bucket__")
        .withPrimaryIndex(true)
        .withQuota(100);

    private static WeakReference<Configuration> configMock;

    @Container
    static final CouchbaseContainer couchbaseContainer =
        new CouchbaseContainer(DockerImageName.parse("couchbase:enterprise").asCompatibleSubstituteFor("couchbase/server"))
            .withCredentials("Administrator", "password")
            .withBucket(testBucketDefinition)
            .withStartupTimeout(Duration.ofMinutes(1));

    private static Cluster cluster;
    public static Cluster getCluster() {
        init();
        return cluster;
    }

    private static void init() {
        if (cluster == null || !couchbaseContainer.isRunning()) {
            if (couchbaseContainer.isRunning()) {
                couchbaseContainer.start();
            }
            cluster = Cluster.connect(
                couchbaseContainer.getConnectionString(),
                couchbaseContainer.getUsername(),
                couchbaseContainer.getPassword()
            );
            configMock.clear();
        }
    }

    public static void clearDatabase() throws Exception {
        Cluster cluster = getCluster();
        executeAndIgnoreException(cluster, "delete from default._default.edgestore");
        executeAndIgnoreException(cluster, "delete from default._default.graphindex");
        executeAndIgnoreException(cluster, "delete from default._default.janusgraph_ids");
        executeAndIgnoreException(cluster, "delete from default._default.system_properties");
        executeAndIgnoreException(cluster, "delete from default._default.systemlog");
        executeAndIgnoreException(cluster, "delete from default._default.txlog");
        executeAndIgnoreException(cluster, "delete from default._default.edgestore_lock_");
        executeAndIgnoreException(cluster, "delete from default._default.graphindex_lock_");
        executeAndIgnoreException(cluster, "delete from default._default.system_properties_lock_");

        Thread.sleep(3000L);

    }

    private static void executeAndIgnoreException(Cluster cluster, String query) {

        try{
            cluster.query(query, queryOptions().scanConsistency(REQUEST_PLUS)).toString();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }



    public static Configuration getConfiguration() {
        init();
        if (configMock == null || configMock.get() == null) {
            Configuration config = Mockito.mock(Configuration.class);
            Mockito.when(config.get(CouchbaseIndexConfigOptions.CLUSTER_CONNECT_STRING)).thenReturn(couchbaseContainer.getConnectionString());
            Mockito.when(config.get(CouchbaseIndexConfigOptions.CLUSTER_CONNECT_USERNAME)).thenReturn(couchbaseContainer.getUsername());
            Mockito.when(config.get(CouchbaseIndexConfigOptions.CLUSTER_CONNECT_PASSWORD)).thenReturn(couchbaseContainer.getPassword());
            Mockito.when(config.get(CouchbaseIndexConfigOptions.CLUSTER_CONNECT_BUCKET)).thenReturn(BUCKET);
            Mockito.when(config.get(CouchbaseIndexConfigOptions.CLUSTER_DEFAULT_SCOPE)).thenReturn(SCOPE);
            Mockito.when(config.get(CouchbaseIndexConfigOptions.CLUSTER_DEFAULT_FUZINESS)).thenReturn(2);
            configMock = new WeakReference<>(config);
        }
        return configMock.get();
    }

    public static Bucket getBucket() {
        return getCluster().bucket(BUCKET);
    }

    public static Scope getScope() {
        return getBucket().scope(SCOPE);
    }
}
