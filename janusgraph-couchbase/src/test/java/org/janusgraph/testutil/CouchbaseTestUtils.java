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

import com.couchbase.client.java.Cluster;

import static com.couchbase.client.java.query.QueryOptions.queryOptions;
import static com.couchbase.client.java.query.QueryScanConsistency.REQUEST_PLUS;

public class CouchbaseTestUtils {

    public static void clearDatabase() throws Exception {
        Cluster cluster = Cluster.connect("localhost", "Administrator", "password");
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
}
