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

package org.janusgraph.diskstorage.couchbase;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.Scope;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.manager.collection.CollectionManager;
import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.query.QueryResult;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.couchbase.mocks.BucketMock;
import org.janusgraph.diskstorage.couchbase.mocks.ClusterMock;
import org.janusgraph.diskstorage.couchbase.mocks.CollectionManagerMock;
import org.janusgraph.diskstorage.couchbase.mocks.ConfigMock;
import org.janusgraph.diskstorage.couchbase.mocks.ScopeMock;
import org.janusgraph.diskstorage.indexing.IndexQuery;
import org.janusgraph.diskstorage.indexing.IndexTransaction;
import org.janusgraph.diskstorage.indexing.KeyInformation;
import org.janusgraph.graphdb.query.condition.Condition;
import org.janusgraph.graphdb.query.condition.FixedCondition;
import org.janusgraph.graphdb.tinkerpop.optimize.step.Aggregation;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

import java.util.Arrays;

@RunWith(PowerMockRunner.class)
@PrepareForTest({
        Cluster.class
})
@Ignore
class CouchbaseIndexTest {
    private Cluster cluster;
    private Bucket bucket;
    private Scope scope;
    private CollectionManager cm;
    private CouchbaseIndex ci;
    private IndexTransaction tx;

    @BeforeEach
    public void setUpTest() {
        cluster = ClusterMock.get();
        bucket = BucketMock.get();
        scope = ScopeMock.get();
        cm = CollectionManagerMock.get();
        ci = Mockito.spy(new CouchbaseIndex(ConfigMock.get()));
        tx = Mockito.mock(IndexTransaction.class);
    }

    @AfterEach
    public void tearDown() {
        System.gc();
    }

    @Test
    void getStorage() {
        Collection cmock = Mockito.mock(Collection.class);
        Mockito.when(scope.collection("__test__")).thenReturn(cmock);
        CouchbaseIndex ci = new CouchbaseIndex(ConfigMock.get());

        Assert.assertEquals(cmock, ci.getStorage("__test__"));

        ci.getStorage("__missing__");
        Mockito.verify(cm).createCollection(Mockito.argThat(cs -> {
            "__missing__".equals(cs.name());
            return true;
        }));
    }

    @Test
    void queryCount() throws BackendException {
        Condition condition = new FixedCondition(true);
        IndexQuery iq = new IndexQuery("test_store", condition);
        KeyInformation.IndexRetriever kiir = Mockito.mock(KeyInformation.IndexRetriever.class);
        QueryResult qr = Mockito.mock(QueryResult.class);
        Mockito.when(qr.rowsAsObject()).thenReturn(Arrays.asList(
                JsonObject.from(ImmutableMap.<String, Object>builder()
                        .put("count", 245L)
                        .build())));

        Mockito.doReturn(qr).when(ci).doQuery(Mockito.anyString(), Mockito.eq(iq), Mockito.any(KeyInformation.IndexRetriever.class), Mockito.eq(tx));

        Number count = ci.queryAggregation(iq, kiir, tx, Aggregation.COUNT);
        Assert.assertEquals(245L, count);
    }

    private void testQuery(IndexQuery iq, KeyInformation.IndexRetriever kiir, String expectedSql, Object expectedArgs) throws BackendException {
        QueryResult qr = Mockito.mock(QueryResult.class);

        Mockito.when(cluster.query(Mockito.eq(expectedSql), Mockito.any(QueryOptions.class)))
                .thenReturn(qr);

        ci.query(iq, kiir, tx);

        Mockito.verify(cluster).query(Mockito.eq(expectedSql), Mockito.argThat(qo -> {
            JsonObject params = JsonObject.create();
            qo.build().injectParams(params);
            Assert.assertEquals(expectedArgs, params.get("args"));
            return true;
        }));
    }

    @Test
    void query() throws BackendException {
        Condition condition = new FixedCondition(true);
        IndexQuery iq = new IndexQuery("test_store", condition);
        KeyInformation.IndexRetriever kiir = Mockito.mock(KeyInformation.IndexRetriever.class);

        testQuery(iq, kiir, "SELECT META().id as id FROM __bucket__.__scope__.test_store WHERE  (true) ", null);
    }

}
