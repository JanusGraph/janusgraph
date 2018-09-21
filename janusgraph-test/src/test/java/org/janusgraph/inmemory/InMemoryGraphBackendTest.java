// Copyright 2018 JanusGraph Authors
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

package org.janusgraph.inmemory;

import org.janusgraph.JanusGraphBackendTestSuite;
import org.janusgraph.JanusGraphIgnoreTest;
import org.janusgraph.JanusGraphProviderClass;
import org.junit.runner.RunWith;

@RunWith(JanusGraphBackendTestSuite.class)
@JanusGraphProviderClass(provider = InMemoryJanusGraphDatabaseProvider.class)
@JanusGraphIgnoreTest(test = "org.janusgraph.graphdb.JanusGraphTest", method = "simpleLogTest", reason = "TODO")
@JanusGraphIgnoreTest(test = "org.janusgraph.graphdb.JanusGraphTest", method = "testLocalGraphConfiguration", reason = "TODO")
@JanusGraphIgnoreTest(test = "org.janusgraph.graphdb.JanusGraphTest", method = "testMaskableGraphConfig", reason = "TODO")
@JanusGraphIgnoreTest(test = "org.janusgraph.graphdb.JanusGraphTest", method = "testGlobalGraphConfig", reason = "TODO")
@JanusGraphIgnoreTest(test = "org.janusgraph.graphdb.JanusGraphTest", method = "testGlobalOfflineGraphConfig", reason = "TODO")
@JanusGraphIgnoreTest(test = "org.janusgraph.graphdb.JanusGraphTest", method = "testFixedGraphConfig", reason = "TODO")
@JanusGraphIgnoreTest(test = "org.janusgraph.graphdb.JanusGraphTest", method = "testManagedOptionMasking", reason = "TODO")
@JanusGraphIgnoreTest(test = "org.janusgraph.graphdb.JanusGraphTest", method = "testTransactionConfiguration", reason = "TODO")
@JanusGraphIgnoreTest(test = "org.janusgraph.graphdb.JanusGraphTest", method = "testTinkerPopOptimizationStrategies", reason = "TODO")
@JanusGraphIgnoreTest(test = "org.janusgraph.graphdb.JanusGraphTest", method = "testDataTypes", reason = "TODO")
@JanusGraphIgnoreTest(test = "org.janusgraph.graphdb.JanusGraphTest", method = "testForceIndexUsage", reason = "TODO")
@JanusGraphIgnoreTest(test = "org.janusgraph.graphdb.JanusGraphTest", method = "testAutomaticTypeCreation", reason = "TODO")
@JanusGraphIgnoreTest(test = "org.janusgraph.graphdb.JanusGraphTest", method = "simpleLogTestWithFailure", reason = "TODO")
@JanusGraphIgnoreTest(test = "org.janusgraph.graphdb.JanusGraphTest", method = "testIndexUpdatesWithReindexAndRemove", reason = "TODO")
@JanusGraphIgnoreTest(test = "org.janusgraph.graphdb.JanusGraphTest", method = "testIndexUpdateSyncWithMultipleInstances", reason = "TODO")
@JanusGraphIgnoreTest(test = "org.janusgraph.graphdb.JanusGraphTest", method = "testClearStorage", reason = "TODO")
@JanusGraphIgnoreTest(test = "org.janusgraph.graphdb.JanusGraphTest", method = "testAutoSchemaMakerForEdgePropertyConstraints", reason = "TODO")
@JanusGraphIgnoreTest(test = "org.janusgraph.graphdb.JanusGraphTest", method = "testAutoSchemaMakerForVertexPropertyConstraints", reason = "TODO")
@JanusGraphIgnoreTest(test = "org.janusgraph.graphdb.JanusGraphTest", method = "testAutoSchemaMakerForConnectionConstraints", reason = "TODO")
@JanusGraphIgnoreTest(test = "org.janusgraph.graphdb.JanusGraphTest", method = "testSupportDirectCommitOfSchemaChangesForVertexProperties", reason = "TODO")
@JanusGraphIgnoreTest(test = "org.janusgraph.graphdb.JanusGraphTest", method = "testSupportDirectCommitOfSchemaChangesForConnection", reason = "TODO")
@JanusGraphIgnoreTest(test = "org.janusgraph.graphdb.JanusGraphTest", method = "testSupportDirectCommitOfSchemaChangesForEdgeProperties", reason = "TODO")

@JanusGraphIgnoreTest(test = "org.janusgraph.graphdb.JanusGraphPartitionGraphTest", method = "testPartitionSpreadFlushBatch", reason = "TODO")
@JanusGraphIgnoreTest(test = "org.janusgraph.graphdb.JanusGraphPartitionGraphTest", method = "testPartitionSpreadFlushNoBatch", reason = "TODO")
@JanusGraphIgnoreTest(test = "org.janusgraph.graphdb.JanusGraphPartitionGraphTest", method = "testKeyBasedGraphPartitioning", reason = "TODO")
@JanusGraphIgnoreTest(test = "org.janusgraph.diskstorage.LockKeyColumnValueStoreTest", method = "testRemoteLockContention", reason = "Does not apply to non-persisting in-memory store")
@JanusGraphIgnoreTest(test = "org.janusgraph.diskstorage.KeyColumnValueStoreTest", method = "testClearStorage", reason = "TODO")

@JanusGraphIgnoreTest(test = "org.janusgraph.graphdb.JanusGraphOperationCountingTest", method = "*", reason = "TODO")
@JanusGraphIgnoreTest(test = "org.janusgraph.graphdb.JanusGraphEventualGraphTest", method = "*", reason = "TODO")
@JanusGraphIgnoreTest(test = "org.janusgraph.graphdb.JanusGraphPerformanceMemoryTest", method = "*", reason = "TODO")
@JanusGraphIgnoreTest(test = "org.janusgraph.graphdb.JanusGraphConcurrentTest", method = "*", reason = "TODO")
@JanusGraphIgnoreTest(test = "org.janusgraph.graphdb.database.management.ManagementTest", method = "*", reason = "TODO")
@JanusGraphIgnoreTest(test = "org.janusgraph.diskstorage.KeyValueStoreTest", method = "*", reason = "TODO")
@JanusGraphIgnoreTest(test = "org.janusgraph.diskstorage.log.KCVSLogTest", method = "*", reason = "TODO")
public class InMemoryGraphBackendTest {
}

