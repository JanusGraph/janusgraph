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

package org.janusgraph.diskstorage.couchbase.mocks;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Scope;
import org.janusgraph.testutil.CouchbaseTestUtils;
import org.mockito.Mockito;

public class ClusterMock {
    public static final String ADDRESS = "__cluster__";
    public static final String USER = "__user__";
    public static final String PASSWORD = "__password__";

    private static Cluster MOCK;

    static {
        Mockito.mockStatic(Cluster.class);
    }

    public static Cluster get() {
        if (MOCK == null) {
            Cluster cluster = Mockito.mock(Cluster.class);
            Bucket bucket = BucketMock.get();
            Scope scope = ScopeMock.get();

            Mockito.when(Cluster.connect(Mockito.eq(ADDRESS), Mockito.eq(USER), Mockito.eq(PASSWORD))).thenReturn(cluster);
            Mockito.when(cluster.bucket(Mockito.eq(CouchbaseTestUtils.BUCKET))).thenReturn(bucket);
            Mockito.when(bucket.scope(CouchbaseTestUtils.SCOPE)).thenReturn(scope);
            Mockito.when(cluster.searchIndexes()).thenReturn(SearchIndexManagerMock.get());
            Mockito.when(cluster.queryIndexes()).thenReturn(QueryIndexManagerMock.get());
            MOCK = cluster;
        }

        return MOCK;
    }

    public static void reset() {
        MOCK = null;
        BucketMock.reset();
        ScopeMock.reset();
        QueryIndexManagerMock.reset();
        SearchIndexManagerMock.reset();
    }
}
