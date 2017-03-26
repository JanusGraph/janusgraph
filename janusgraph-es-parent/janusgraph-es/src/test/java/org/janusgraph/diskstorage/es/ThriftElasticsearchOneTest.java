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
package org.janusgraph.diskstorage.es;

import java.util.concurrent.ExecutionException;

import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import org.elasticsearch.test.ElasticsearchIntegrationTest.Scope;

import org.janusgraph.CassandraStorageSetup;
import org.janusgraph.StorageSetup;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.es.ThriftEsShim;
import org.janusgraph.testcategory.BrittleTests;
import org.janusgraph.testutil.TestGraphConfigs;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.janusgraph.CassandraStorageSetup.*;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.INDEX_BACKEND;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.INDEX_DIRECTORY;

@ClusterScope(scope = Scope.TEST, numDataNodes = 0)
public class ThriftElasticsearchOneTest extends ElasticsearchIntegrationTest  {
    @BeforeClass
    public static void beforeClass() {
        CassandraStorageSetup.startCleanEmbedded();
    }

    ThriftEsShim shim = new ThriftEsShim();

    @Test
    public void testGraphOfTheGods() {
        shim.testGraphOfTheGods();
    }

    @Test
    public void testSimpleUpdate() {
        shim.testSimpleUpdate();
    }

    @Test
    public void testIndexing() {
        shim.testIndexing();
    }

    @Test
    public void testBooleanIndexing() {
        shim.testBooleanIndexing();
    }

    @Test
    public void testDateIndexing() {
        shim.testDateIndexing();
    }

    @Test
    public void testInstantIndexing() {
        shim.testInstantIndexing();
    }

    @Test
    public void testUUIDIndexing() {
        shim.testUUIDIndexing();
    }

    @Test
    public void testConditionalIndexing() {
        shim.testConditionalIndexing();
    }

    @Test
    public void testCompositeAndMixedIndexing() {
        shim.testCompositeAndMixedIndexing();
    }

    @Test
    public void testIndexParameters() {
        shim.testIndexParameters();
    }

    @Test
    public void testRawQueries() {
        shim.testRawQueries();
    }

    @Test
    public void testDualMapping() {
        shim.testDualMapping();
    }

    //TODO add BrittleTests category
    @Test
    public void testIndexReplay() throws Exception {
        shim.testIndexReplay();
    }

    @Test
    public void testIndexUpdatesWithoutReindex() throws InterruptedException, ExecutionException {
        shim.testIndexUpdatesWithoutReindex();
    }

    //ES 5 doesn't support TTL, skipping
    //public void testVertexTTLWithMixedIndices() throws Exception {

    @Test
    public void testDeleteVertexThenDeleteProperty() throws BackendException {
        shim.testDeleteVertexThenDeleteProperty();
    }

    @Test
    public void testDeleteVertexThenAddProperty() throws BackendException {
        shim.testDeleteVertexThenAddProperty();
    }

    @Test
    public void testDeleteVertexThenModifyProperty() throws BackendException {
        shim.testDeleteVertexThenModifyProperty();
    }

    @Test
    public void testIndexQueryWithScore() throws InterruptedException {
        shim.testIndexQueryWithScore();
    }

    @Test
    public void testContainsWithMultipleValues() throws Exception {
        shim.testContainsWithMultipleValues();
    }

    @Test
    public void testWidcardQuery() {
        shim.testWildcardQuery();
    }

    @Test
    public void testListIndexing() {
        shim.testListIndexing();
    }

    @Test
    public void testSetIndexing() {
        shim.testSetIndexing();
    }

  }
