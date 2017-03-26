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
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.graphdb.JanusGraphIndexTest;
import org.janusgraph.StorageSetup;

import static org.janusgraph.CassandraStorageSetup.*;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.INDEX_BACKEND;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.INDEX_DIRECTORY;

/*
 *  This is bit of a hack, but annotations aren't inherited, so it allows us to extend EsIntegTestCase
 *  on the main classes
 */

class ThriftEsShim extends JanusGraphIndexTest {

    public ThriftEsShim() {
        super(true, true, true);
    }

    @Override
    public WriteConfiguration getConfiguration() {
        ModifiableConfiguration config =
                getCassandraThriftConfiguration(ThriftEsShim.class.getName());
        //Add index
        config.set(INDEX_BACKEND,"elasticsearch",INDEX);
        config.set(INDEX_DIRECTORY, StorageSetup.getHomeDir("es"),INDEX);
        return config.getConfiguration();
    }

    @Override
    public boolean supportsLuceneStyleQueries() {
        return true;
    }

    @Override
    public boolean supportsWildcardQuery() {
        return true;
    }
    @Override
    protected boolean supportsCollections() {
        return true;
    }
    @Override
    public void testGraphOfTheGods() {
        superTestGraphOfTheGods();
    }
    @Override
    public void testSimpleUpdate() {
        superTestSimpleUpdate();
    }
    @Override
    public void testIndexing() {
        superTestIndexing();
    }
    @Override
    public void testBooleanIndexing() {
        superTestBooleanIndexing();
    }
    @Override
    public void testDateIndexing() {
        superTestDateIndexing();
    }
    @Override
    public void testInstantIndexing() {
        superTestInstantIndexing();
    }
    @Override
    public void testUUIDIndexing() {
        superTestUUIDIndexing();
    }
    @Override
    public void testConditionalIndexing() {
        superTestConditionalIndexing();
    }
    @Override
    public void testCompositeAndMixedIndexing() {
        superTestCompositeAndMixedIndexing();
    }
    @Override
    public void testIndexParameters() {
        superTestIndexParameters();
    }
    @Override
    public void testRawQueries() {
        superTestRawQueries();
    }
    @Override
    public void testDualMapping() {
        superTestDualMapping();
    }
    @Override
    public void testIndexReplay() throws Exception {
        superTestIndexReplay();
    }
    @Override
    public void testIndexUpdatesWithoutReindex() throws InterruptedException, ExecutionException {
        superTestIndexUpdatesWithoutReindex();
    }
    @Override
    public void testVertexTTLWithMixedIndices() throws Exception {
        superTestVertexTTLWithMixedIndices();
    }
    @Override
    public void testEdgeTTLWithMixedIndices() throws Exception {
        superTestEdgeTTLWithMixedIndices();
    }
    @Override
    public void testDeleteVertexThenDeleteProperty() throws BackendException {
        superTestDeleteVertexThenDeleteProperty();
    }
    @Override
    public void testDeleteVertexThenAddProperty() throws BackendException {
        superTestDeleteVertexThenAddProperty();
    }
    @Override
    public void testDeleteVertexThenModifyProperty() throws BackendException {
        superTestDeleteVertexThenModifyProperty();
    }
    @Override
    public void testIndexQueryWithScore() throws InterruptedException {
        superTestIndexQueryWithScore();
    }

    // superTests a case when there as AND with a single CONTAINS condition inside AND(name:(was here))
    @Override
    public void testWildcardQuery() {
        superTestWildcardQuery();
    }
    @Override
    public void testListIndexing() {
        superTestListIndexing();
    }
    @Override
    public void testSetIndexing() {
        superTestSetIndexing();
    }


    private void superTestGraphOfTheGods() {
        super.testGraphOfTheGods();
    }

    private void superTestSimpleUpdate() {
        super.testSimpleUpdate();
    }

    private void superTestIndexing() {
        super.testIndexing();
    }

    private void superTestBooleanIndexing() {
        super.testBooleanIndexing();
    }

    private void superTestDateIndexing() {
        super.testDateIndexing();
    }

    private void superTestInstantIndexing() {
        super.testInstantIndexing();
    }

    private void superTestUUIDIndexing() {
        super.testUUIDIndexing();
    }

    private void superTestConditionalIndexing() {
        super.testConditionalIndexing();
    }

    private void superTestCompositeAndMixedIndexing() {
        super.testCompositeAndMixedIndexing();
    }

    private void superTestIndexParameters() {
        super.testIndexParameters();
    }

    private void superTestRawQueries() {
        super.testRawQueries();
    }

    private void superTestDualMapping() {
        super.testDualMapping();
    }

    private void superTestIndexReplay() throws Exception {
        super.testIndexReplay();
    }

    private void superTestIndexUpdatesWithoutReindex() throws InterruptedException, ExecutionException {
        super.testIndexUpdatesWithoutReindex();
    }

    private void superTestVertexTTLWithMixedIndices() throws Exception {
        super.testVertexTTLWithMixedIndices();
    }

    private void superTestEdgeTTLWithMixedIndices() throws Exception {
        super.testEdgeTTLWithMixedIndices();
    }

    private void superTestDeleteVertexThenDeleteProperty() throws BackendException {
        super.testDeleteVertexThenDeleteProperty();
    }

    private void superTestDeleteVertexThenAddProperty() throws BackendException {
        super.testDeleteVertexThenAddProperty();
    }

    private void superTestDeleteVertexThenModifyProperty() throws BackendException {
        super.testDeleteVertexThenModifyProperty();
    }

    private void superTestIndexQueryWithScore() throws InterruptedException {
        super.testIndexQueryWithScore();
    }

    private void superTestWildcardQuery() {
        super.testWildcardQuery();
    }

    private void superTestListIndexing() {
        super.testListIndexing();
    }

    private void superTestSetIndexing() {
        super.testSetIndexing();

    }

}
