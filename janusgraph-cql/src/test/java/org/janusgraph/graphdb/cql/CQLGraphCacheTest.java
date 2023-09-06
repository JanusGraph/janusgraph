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

package org.janusgraph.graphdb.cql;

import io.github.artsok.ParameterizedRepeatedIfExceptionsTest;
import io.github.artsok.RepeatedIfExceptionsTest;
import org.janusgraph.JanusGraphCassandraContainer;
import org.janusgraph.StorageSetup;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.graphdb.JanusGraphTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.concurrent.ExecutionException;

@Testcontainers
public class CQLGraphCacheTest extends JanusGraphTest {

    @Container
    public static final JanusGraphCassandraContainer cqlContainer = new JanusGraphCassandraContainer();

    @Override
    public WriteConfiguration getConfiguration() {
        return StorageSetup.addPermanentCache(cqlContainer.getConfiguration(getClass().getSimpleName()));
    }

    // flaky test: https://github.com/JanusGraph/janusgraph/issues/1498
    @RepeatedIfExceptionsTest(repeats = 3)
    @Override
    public void testIndexUpdatesWithReindexAndRemove() throws InterruptedException, ExecutionException {
        super.testIndexUpdatesWithReindexAndRemove();
    }

    // flaky test: https://github.com/JanusGraph/janusgraph/issues/1457
    @ParameterizedRepeatedIfExceptionsTest(repeats = 4, minSuccess = 2)
    @ValueSource(booleans = {true, false})
    @Override
    public void simpleLogTest(boolean useStringId) throws InterruptedException {
        super.simpleLogTest(useStringId);
    }

    // flaky test: https://github.com/JanusGraph/janusgraph/issues/1457
    @ParameterizedRepeatedIfExceptionsTest(repeats = 4, minSuccess = 2)
    @ValueSource(booleans = {true, false})
    @Override
    public void simpleLogTestWithFailure(boolean useStringId) throws InterruptedException {
        super.simpleLogTestWithFailure(useStringId);
    }

    // flaky test: https://github.com/JanusGraph/janusgraph/issues/1497
    @RepeatedIfExceptionsTest(repeats = 3)
    @Override
    public void testEdgeTTLTiming() throws Exception {
        super.testEdgeTTLTiming();
    }

    // flaky test: https://github.com/JanusGraph/janusgraph/issues/1462
    @RepeatedIfExceptionsTest(repeats = 3)
    @Override
    public void testEdgeTTLWithTransactions() throws Exception {
        super.testEdgeTTLWithTransactions();
    }

    // flaky test: https://github.com/JanusGraph/janusgraph/issues/1464
    @RepeatedIfExceptionsTest(repeats = 3)
    @Override
    public void testVertexTTLWithCompositeIndex() throws Exception {
        super.testVertexTTLWithCompositeIndex();
    }

    // flaky test: https://github.com/JanusGraph/janusgraph/issues/1465
    @RepeatedIfExceptionsTest(repeats = 3)
    @Override
    public void testVertexTTLImplicitKey() throws Exception {
        super.testVertexTTLImplicitKey();
    }

    // flaky test: https://github.com/JanusGraph/janusgraph/issues/3142
    @RepeatedIfExceptionsTest(repeats = 3)
    @Override
    public void testReindexingForEdgeIndex() throws ExecutionException, InterruptedException {
        super.testReindexingForEdgeIndex();
    }
}
