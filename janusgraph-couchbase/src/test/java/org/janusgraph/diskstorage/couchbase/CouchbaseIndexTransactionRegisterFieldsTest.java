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

import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.manager.query.CreateQueryIndexOptions;
import com.couchbase.client.java.manager.query.QueryIndexManager;
import com.couchbase.client.java.manager.search.SearchIndexManager;
import org.janusgraph.core.schema.Mapping;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.couchbase.mocks.QueryIndexManagerMock;
import org.janusgraph.diskstorage.couchbase.mocks.SearchIndexManagerMock;
import org.janusgraph.diskstorage.indexing.KeyInformation;
import org.janusgraph.testutil.CouchbaseTestUtils;
import org.junit.Assert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.time.Instant;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

@RunWith(PowerMockRunner.class)
@PrepareForTest({
        Mapping.class,
        CreateQueryIndexOptions.class,
        CouchbaseIndexTransaction.class
})
class CouchbaseIndexTransactionRegisterFieldsTest {
    CouchbaseIndexTransaction cit;
    Cluster cluster;
    CreateQueryIndexOptions cqi;

    @BeforeAll
    void prepareAll() {
        cluster = CouchbaseTestUtils.getCluster();
//        cit = new CouchbaseIndexTransaction(new StandardBaseTransactionConfig(
//            "couchbase-test", TimestampProviders.MILLI, Instant.now(), null
//        ));
    }

    @AfterEach
    void tearDown() throws Exception {
        CouchbaseTestUtils.clearDatabase();
    }

    private KeyInformation keyInformation(Mapping mapping, Class type) {
        KeyInformation ki = Mockito.mock(KeyInformation.class);
        Mockito.when(Mapping.getMapping(ki)).thenReturn(mapping);
        Mockito.when(ki.getDataType()).thenReturn(type);
        return ki;
    }

    private void testMapping(Mapping mapping, Class type, String expectedFtsType) throws BackendException {
        cit.register("test_store", "test_key", keyInformation(mapping, type));
        cit.commit();

        QueryIndexManager qim = QueryIndexManagerMock.get();
        HashSet<String> expectedKeys = new HashSet<>();
        expectedKeys.add("`test_key`");
        Mockito.verify(qim).createIndex(Mockito.eq(CouchbaseTestUtils.BUCKET), Mockito.eq("__inp__test_store"), Mockito.eq(expectedKeys), Mockito.eq(cqi));
        Mockito.verify(cluster, Mockito.times(2)).queryIndexes();

        SearchIndexManager sim = SearchIndexManagerMock.get();
        Mockito.verify(sim).upsertIndex(Mockito.argThat(si -> {
            Assert.assertNotNull(si);
            Map<String, Object> params = si.params();
            Assert.assertNotNull(params);
            List<Map<String, Object>> keyProps = (List<Map<String, Object>>) pullMapKeys(params, "mapping/types/__scope__.test_store/properties/test_key/fields");
            AtomicBoolean found = new AtomicBoolean(false);
            keyProps.stream()
                    .filter(kp -> "test_key".equals(kp.get("name")))
                    .findFirst().ifPresent(kp -> {
                        Assert.assertEquals(expectedFtsType, kp.get("type"));
                        found.set(true);
                    });
            if (!found.get()) {
                throw new RuntimeException();
            }
            return true;
        }));
    }

    private Object pullMapKeys(Map<String, Object> params, String path) {
        String[] keys = path.split("\\/");
        for (int i = 0; i < keys.length; i++) {
            try {
                if (!params.containsKey(keys[i])) {
                    throw new IllegalArgumentException();
                }
                if (i == keys.length - 1) {
                    return params.get(keys[i]);
                }
                params = (Map<String, Object>) params.get(keys[i]);
            } catch (Exception e) {
                throw new RuntimeException("Failed to pull key '" + keys[i] + "'; possible keys: " + params.keySet());
            }
        }
        return params;
    }

    @Test
    void testDefaultStringMapping() throws BackendException {
        testMapping(Mapping.DEFAULT, String.class, "text");
    }

    @Test
    void testTextStringMapping() throws BackendException {
        testMapping(Mapping.TEXT, String.class, "text");
    }

    @Test
    void testTextStringStringMapping() throws BackendException {
        testMapping(Mapping.TEXTSTRING, String.class, "text");
    }

    @Test
    void testNumberMapping() throws BackendException {
        testMapping(Mapping.DEFAULT, Double.class, "number");
    }

    @Test
    void testBooleanMapping() throws BackendException {
        testMapping(Mapping.DEFAULT, Boolean.class, "boolean");
    }

    @Test
    void testDateMapping() throws BackendException {
        testMapping(Mapping.DEFAULT, Date.class, "datetime");
    }

    @Test
    void testInstantMapping() throws BackendException {
        testMapping(Mapping.DEFAULT, Instant.class, "datetime");
    }

}
