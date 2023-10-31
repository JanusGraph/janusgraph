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

package org.janusgraph.graphdb;

import org.janusgraph.CouchbaseStorageSetup;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.testutil.CouchbaseTestUtils;
import org.junit.Test;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.TestInfo;

import java.nio.file.Path;
import java.util.concurrent.ExecutionException;

//@Testcontainers
public class CouchbaseGraphTest extends JanusGraphTest {
    @AfterEach
    public void teardown(TestInfo testInfo) throws Exception {
        super.setUp(testInfo);
        CouchbaseTestUtils.clearDatabase();
    }

    @Override
    public WriteConfiguration getConfiguration() {
        return CouchbaseStorageSetup.getCouchbaseConfiguration();
    }

    //TODO: Fix Test
    @Disabled
    @Override
    public void testBasic() throws BackendException {}


    //TODO: Fix Test
    @Disabled
    @Override
    public void testIndexUpdatesWithReindexAndRemove() throws InterruptedException, ExecutionException{}


    @Override
    @Test
    public void testUpdateVertexPropThenRemoveProp() {
        super.testUpdateVertexPropThenRemoveProp();
    }

    @Override
    public void testNestedAddVertexPropThenRemoveProp() {
        super.testNestedAddVertexPropThenRemoveProp();
    }

    @Override
    public void testUpdateVertexPropThenRemoveVertex() {
        super.testUpdateVertexPropThenRemoveVertex();
    }

    @Override
    public void testUpdatePropertyPropThenRemoveProperty() {
        super.testUpdatePropertyPropThenRemoveProperty();
    }

    @Override
    public void testUpdatePropertyPropThenRemovePropertyProp() {
        super.testUpdatePropertyPropThenRemovePropertyProp();
    }

    @Override
    public void testUpdatePropertyPropThenRemoveVertex() {
        super.testUpdatePropertyPropThenRemoveVertex();
    }

    @Override
    public void testUpdateEdgePropertyThenRemoveEdge() {
        super.testUpdateEdgePropertyThenRemoveEdge();
    }

    @Override
    public void testUpdateForkEdgePropertyThenRemoveEdge() {
        super.testUpdateForkEdgePropertyThenRemoveEdge();
    }

    @Override
    public void testUpdateForkEdgePropertyThenFindEdgeById() {
        super.testUpdateForkEdgePropertyThenFindEdgeById();
    }

    @Override
    public void testOpenClose() {
        super.testOpenClose();
    }

    @Override
    public void testClearStorage() throws Exception {
        super.testClearStorage();
    }

    @Override
    public void testVertexRemoval() {
        super.testVertexRemoval();
    }

    @Override
    public void testGlobalIteration() {
        super.testGlobalIteration();
    }

    @Override
    public void testMediumCreateRetrieve() {
        super.testMediumCreateRetrieve();
    }

    @Override
    public void testSchemaTypes() {
        super.testSchemaTypes();
    }

    @Override
    public void testDataTypes() {
        super.testDataTypes();
    }

    @Override
    public <T> void testSupportOfDataTypes(Class<T> classes, T data, Equals<T> a) {
        super.testSupportOfDataTypes(classes, data, a);
    }

    @Override
    public void testTransactionalScopeOfSchemaTypes() {
        super.testTransactionalScopeOfSchemaTypes();
    }

    @Override
    public void testDefaultSchemaMaker() {
        super.testDefaultSchemaMaker();
    }

    @Override
    public void testDisableDefaultSchemaMaker() {
        super.testDisableDefaultSchemaMaker();
    }

    @Override
    public void testIgnorePropertySchemaMaker() {
        super.testIgnorePropertySchemaMaker();
    }

    @Override
    public void testUpdateSchemaChangeNameForEdgeLabel() {
        super.testUpdateSchemaChangeNameForEdgeLabel();
    }

    @Override
    public void testUpdateSchemaChangeNameForVertexLabel() {
        super.testUpdateSchemaChangeNameForVertexLabel();
    }

    @Override
    public void testUpdateSchemaChangeNameForPropertyKey() {
        super.testUpdateSchemaChangeNameForPropertyKey();
    }

    @Override
    public void testUpdateSchemaChangeNameForCompositeIndex() {
        super.testUpdateSchemaChangeNameForCompositeIndex();
    }

    @Override
    public void testUpdateSchemaChangeNameForRelationIndex() {
        super.testUpdateSchemaChangeNameForRelationIndex();
    }

    @Override
    public void testGotGLoadWithoutIndexBackendException() {
        super.testGotGLoadWithoutIndexBackendException();
    }

    @Override
    public void testGotGIndexRemoval() throws InterruptedException, ExecutionException {
        super.testGotGIndexRemoval();
    }

    @Override
    public void testVertexCentricEdgeIndexOnSimpleMultiplicityShouldWork() {
        super.testVertexCentricEdgeIndexOnSimpleMultiplicityShouldWork();
    }

    @Override
    public void testVertexCentricPropertyIndexOnSetCardinalityShouldWork() {
        super.testVertexCentricPropertyIndexOnSetCardinalityShouldWork();
    }

    @Override
    public void testVertexCentricIndexOrderingOnEdgePropertyWithCardinalityList() {
        super.testVertexCentricIndexOrderingOnEdgePropertyWithCardinalityList();
    }

    @Override
    public void testVertexCentricIndexOrderingOnMetaPropertyWithCardinalityList() {
        super.testVertexCentricIndexOrderingOnMetaPropertyWithCardinalityList();
    }

    @Override
    public void testIndexUpdateSyncWithMultipleInstances() throws InterruptedException {
        super.testIndexUpdateSyncWithMultipleInstances();
    }

    @Override
    public void testIndexShouldRegisterWhenWeRemoveAnInstance() throws InterruptedException {
        super.testIndexShouldRegisterWhenWeRemoveAnInstance();
    }

    @Override
    public void testIndexShouldBeEnabledForExistingPropertyKeyAndConstrainedToNewVertexLabel() {
        super.testIndexShouldBeEnabledForExistingPropertyKeyAndConstrainedToNewVertexLabel();
    }

    @Override
    public void testIndexShouldBeEnabledForExistingPropertyKeyAndConstrainedToNewEdgeLabel() {
        super.testIndexShouldBeEnabledForExistingPropertyKeyAndConstrainedToNewEdgeLabel();
    }

    @Override
    public void testIndexShouldNotBeEnabledForExistingPropertyKeyAndConstrainedToExistingVertexLabel() {
        super.testIndexShouldNotBeEnabledForExistingPropertyKeyAndConstrainedToExistingVertexLabel();
    }

    @Override
    public void testIndexShouldNotBeEnabledForExistingPropertyKeyAndConstrainedToExistingEdgeLabel() {
        super.testIndexShouldNotBeEnabledForExistingPropertyKeyAndConstrainedToExistingEdgeLabel();
    }

    @Override
    public void testIndexShouldNotBeEnabledForExistingPropertyKeyWithoutLabelConstraint() {
        super.testIndexShouldNotBeEnabledForExistingPropertyKeyWithoutLabelConstraint();
    }

    @Override
    public void testRelationTypeIndexShouldBeEnabledForExistingPropertyKeyAndNewRelationType() {
        super.testRelationTypeIndexShouldBeEnabledForExistingPropertyKeyAndNewRelationType();
    }

    @Override
    public void testRelationTypeIndexShouldBeEnabledForNewPropertyKeyAndExistingRelationType() {
        super.testRelationTypeIndexShouldBeEnabledForNewPropertyKeyAndExistingRelationType();
    }

    @Override
    public void testRelationTypeIndexShouldBeEnabledForSingleNewPropertyKeyAndExistingRelationType() {
        super.testRelationTypeIndexShouldBeEnabledForSingleNewPropertyKeyAndExistingRelationType();
    }

    @Override
    public void testRelationTypeIndexShouldBeEnabledForSingleNewPropertyKeyAndNewRelationType() {
        super.testRelationTypeIndexShouldBeEnabledForSingleNewPropertyKeyAndNewRelationType();
    }

    @Override
    public void testRelationTypeIndexShouldBeEnabledForNewPropertyKeyAndNewRelationType() {
        super.testRelationTypeIndexShouldBeEnabledForNewPropertyKeyAndNewRelationType();
    }

    @Override
    public void testRelationTypeIndexShouldNotBeEnabledForExistingPropertyKeyAndExistingRelationType() {
        super.testRelationTypeIndexShouldNotBeEnabledForExistingPropertyKeyAndExistingRelationType();
    }

    @Override
    public void testPropertyCardinality() {
        super.testPropertyCardinality();
    }

    @Override
    public void testImplicitKey() {
        super.testImplicitKey();
    }

    @Override
    public void testArrayEqualityUsingImplicitKey() {
        super.testArrayEqualityUsingImplicitKey();
    }

    @Override
    public void testSelfLoop() {
        super.testSelfLoop();
    }

    @Override
    public void testThreadBoundTx() {
        super.testThreadBoundTx();
    }

    @Override
    public void testPropertyIdAccessInDifferentTransaction() {
        super.testPropertyIdAccessInDifferentTransaction();
    }

    @Override
    public void testCacheForceRefresh() {
        super.testCacheForceRefresh();
    }

    @Override
    public void testTransactionScopeTransition() {
        super.testTransactionScopeTransition();
    }

    @Override
    public void testNestedTransactions() {
        super.testNestedTransactions();
    }

    @Override
    public void testStaleVertex() {
        super.testStaleVertex();
    }

    @Override
    public void testTransactionIsolation() {
        super.testTransactionIsolation();
    }

    @Override
    public <V> void testMultivaluedVertexProperty() {
        super.testMultivaluedVertexProperty();
    }

    @Override
    public void testLocalGraphConfiguration() {
        super.testLocalGraphConfiguration();
    }

    @Override
    public void testMaskableGraphConfig() {
        super.testMaskableGraphConfig();
    }

    @Override
    public void testGlobalGraphConfig() {
        super.testGlobalGraphConfig();
    }

    @Override
    public void testGlobalOfflineGraphConfig() {
        super.testGlobalOfflineGraphConfig();
    }

    @Override
    public void testFixedGraphConfig() {
        super.testFixedGraphConfig();
    }

    @Override
    public void testManagedOptionMasking() throws BackendException {
        super.testManagedOptionMasking();
    }

    @Override
    public void testTransactionConfiguration() {
        super.testTransactionConfiguration();
    }

    @Override
    public void testConsistencyEnforcement() {
        super.testConsistencyEnforcement();
    }

    @Override
    public void testConcurrentConsistencyEnforcement() throws Exception {
        super.testConcurrentConsistencyEnforcement();
    }

    @Override
    public void testVertexCentricQuery() {
        super.testVertexCentricQuery();
    }

    @Override
    public void testVertexCentricQuery(int noVertices) {
        super.testVertexCentricQuery(noVertices);
    }

    @Override
    public void testRelationTypeIndexes() {
        super.testRelationTypeIndexes();
    }

    @Override
    public void testAutoSchemaMakerAllowsToSetCardinalityList() {
        super.testAutoSchemaMakerAllowsToSetCardinalityList();
    }

    @Override
    public void testAutoSchemaMakerAllowsToSetCardinalitySet() {
        super.testAutoSchemaMakerAllowsToSetCardinalitySet();
    }

    @Override
    public void testAutoSchemaMakerAllowsToSetCardinalitySingle() {
        super.testAutoSchemaMakerAllowsToSetCardinalitySingle();
    }

    @Override
    public void testEnforcedSchemaAllowsDefinedVertexProperties() {
        super.testEnforcedSchemaAllowsDefinedVertexProperties();
    }

    @Override
    public void testSchemaIsEnforcedForVertexProperties() {
        super.testSchemaIsEnforcedForVertexProperties();
    }

    @Override
    public void testAllowDisablingSchemaConstraintForVertexProperty() {
        super.testAllowDisablingSchemaConstraintForVertexProperty();
    }

    @Override
    public void testAllowDisablingSchemaConstraintForConnection() {
        super.testAllowDisablingSchemaConstraintForConnection();
    }

    @Override
    public void testAllowDisablingSchemaConstraintForEdgeProperty() {
        super.testAllowDisablingSchemaConstraintForEdgeProperty();
    }

    @Override
    public void testAutoSchemaMakerForVertexPropertyConstraints() {
        super.testAutoSchemaMakerForVertexPropertyConstraints();
    }

    @Override
    public void testSupportDirectCommitOfSchemaChangesForVertexProperties() {
        super.testSupportDirectCommitOfSchemaChangesForVertexProperties();
    }

    @Override
    public void testSupportDirectCommitOfSchemaChangesForConnection() {
        super.testSupportDirectCommitOfSchemaChangesForConnection();
    }

    @Override
    public void testSupportDirectCommitOfSchemaChangesForEdgeProperties() {
        super.testSupportDirectCommitOfSchemaChangesForEdgeProperties();
    }

    @Override
    public void testEnforcedSchemaAllowsDefinedEdgeProperties() {
        super.testEnforcedSchemaAllowsDefinedEdgeProperties();
    }

    @Override
    public void testSchemaIsEnforcedForEdgeProperties() {
        super.testSchemaIsEnforcedForEdgeProperties();
    }

    @Override
    public void testAllowSingleCardinalityForEdgeProperties() {
        super.testAllowSingleCardinalityForEdgeProperties();
    }

    @Override
    public void testBanListCardinalityForEdgeProperties() {
        super.testBanListCardinalityForEdgeProperties();
    }

    @Override
    public void testBanSetCardinalityForEdgeProperties() {
        super.testBanSetCardinalityForEdgeProperties();
    }

    @Override
    public void testAutoSchemaMakerForEdgePropertyConstraints() {
        super.testAutoSchemaMakerForEdgePropertyConstraints();
    }

    @Override
    public void testEnforcedSchemaAllowsDefinedConnections() {
        super.testEnforcedSchemaAllowsDefinedConnections();
    }

    @Override
    public void testSchemaIsEnforcedForConnections() {
        super.testSchemaIsEnforcedForConnections();
    }

    @Override
    public void testAutoSchemaMakerForConnectionConstraints() {
        super.testAutoSchemaMakerForConnectionConstraints();
    }

    @Override
    public void testSupportChangeNameOfEdgeAndUpdateConnections() {
        super.testSupportChangeNameOfEdgeAndUpdateConnections();
    }

    @Override
    public void testAllowEnforcedComplexConnections() {
        super.testAllowEnforcedComplexConnections();
    }

    @Override
    public void testEnforceComplexConnections() {
        super.testEnforceComplexConnections();
    }

    @Override
    public void testEdgesExceedCacheSize() {
        super.testEdgesExceedCacheSize();
    }

    @Override
    public void testRemoveCachedVertexVisibility() {
        super.testRemoveCachedVertexVisibility();
    }

    @Override
    public void testNestedContainPredicates() {
        super.testNestedContainPredicates();
    }

    @Override
    public void testTinkerPopCardinality() {
        super.testTinkerPopCardinality();
    }

    @Override
    public void testMultiQueryMetricsWhenReadingFromBackend() {
        super.testMultiQueryMetricsWhenReadingFromBackend();
    }

    @Override
    public void testLimitBatchSizeForMultiQuery() {
        super.testLimitBatchSizeForMultiQuery();
    }

    @Override
    public void testSimpleTinkerPopTraversal() {
        super.testSimpleTinkerPopTraversal();
    }

    @Override
    public void testHasKeyOnEdgePropertyTraversal() {
        super.testHasKeyOnEdgePropertyTraversal();
    }

    @Override
    public void testHasValueOnEdgePropertyTraversal() {
        super.testHasValueOnEdgePropertyTraversal();
    }

    @Override
    public void testHasKeyAndHasValueOnEdgePropertyTraversal() {
        super.testHasKeyAndHasValueOnEdgePropertyTraversal();
    }

    @Override
    public void testBatchPropertiesPrefetching(int txCacheSize) {
        super.testBatchPropertiesPrefetching(txCacheSize);
    }

    @Override
    public void testBatchPropertiesPrefetchingFromEdges(int txCacheSize) {
        super.testBatchPropertiesPrefetchingFromEdges(txCacheSize);
    }

    public void simpleLogTestWithFailure() throws InterruptedException {
        super.simpleLogTestWithFailure(false);
    }

    public void simpleLogTest(boolean withLogFailure) throws InterruptedException {
        super.simpleLogTest(withLogFailure);
    }

    @Override
    public void testGlobalGraphIndexingAndQueriesForInternalIndexes() {
        super.testGlobalGraphIndexingAndQueriesForInternalIndexes();
    }

    @Override
    public void testTinkerPropInfinityLimit() {
        super.testTinkerPropInfinityLimit();
    }

    @Override
    public void testTinkerPopTextContainingFindsCorrectValue() {
        super.testTinkerPopTextContainingFindsCorrectValue();
    }

    @Override
    public void testTinkerPopTextContainingFindsRightNumberOfValues() {
        super.testTinkerPopTextContainingFindsRightNumberOfValues();
    }

    @Override
    public void testTinkerPopTextPredicatesConnectedViaAnd() {
        super.testTinkerPopTextPredicatesConnectedViaAnd();
    }

    @Override
    public void testTinkerPopTextStartingWith() {
        super.testTinkerPopTextStartingWith();
    }

    @Override
    public void testIndexUniqueness() {
        super.testIndexUniqueness();
    }

    @Override
    public void testForceIndexUsage() {
        super.testForceIndexUsage();
    }

    @Override
    public void testLargeJointIndexRetrieval() {
        super.testLargeJointIndexRetrieval();
    }

    @Override
    public void testIndexQueryWithLabelsAndContainsIN() {
        super.testIndexQueryWithLabelsAndContainsIN();
    }

    @Override
    public void testLimitWithMixedIndexCoverage() {
        super.testLimitWithMixedIndexCoverage();
    }

    @Override
    public void testWithoutIndex() {
        super.testWithoutIndex();
    }

    @Override
    public void testNeqQuery() {
        super.testNeqQuery();
    }

    @Override
    public void testHasNullQuery() {
        super.testHasNullQuery();
    }

    @Override
    public void testNullValueMutation() {
        super.testNullValueMutation();
    }

    @Override
    public void testHasNot() {
        super.testHasNot();
    }

    @Override
    public void testNotHas() {
        super.testNotHas();
    }

    @Override
    public void testGraphCentricQueryProfiling() {
        super.testGraphCentricQueryProfiling();
    }

    @Override
    public void testGraphCentricQueryProfilingWithLimitAdjusting() throws BackendException {
        super.testGraphCentricQueryProfilingWithLimitAdjusting();
    }

    @Override
    public void testVertexCentricQueryProfiling() {
        super.testVertexCentricQueryProfiling();
    }

    @Override
    public void testVertexCentricIndexWithNull() {
        super.testVertexCentricIndexWithNull();
    }

    @Override
    public void testCreateDelete() {
        super.testCreateDelete();
    }

    @Override
    public void testRemoveEdge() {
        super.testRemoveEdge();
    }

    @Override
    public void testEdgeTTLTiming() throws Exception {
        super.testEdgeTTLTiming();
    }

    @Override
    public void testEdgeTTLWithTransactions() throws Exception {
        super.testEdgeTTLWithTransactions();
    }

    @Override
    public void testEdgeTTLWithIndex() throws Exception {
        super.testEdgeTTLWithIndex();
    }

    @Override
    public void testPropertyTTLTiming() throws Exception {
        super.testPropertyTTLTiming();
    }

    @Override
    public void testVertexTTLWithCompositeIndex() throws Exception {
        super.testVertexTTLWithCompositeIndex();
    }

    @Override
    public void testEdgeTTLLimitedByVertexTTL() throws Exception {
        super.testEdgeTTLLimitedByVertexTTL();
    }

    @Override
    public void testSettingTTLOnUnsupportedType() {
        super.testSettingTTLOnUnsupportedType();
    }

    @Override
    public void testUnsettingTTL() throws InterruptedException {
        super.testUnsettingTTL();
    }

    @Override
    public void testGettingUndefinedEdgeLabelTTL() {
        super.testGettingUndefinedEdgeLabelTTL();
    }

    @Override
    public void testGettingUndefinedVertexLabelTTL() {
        super.testGettingUndefinedVertexLabelTTL();
    }

    @Override
    public void testGetTTLFromUnsupportedType() {
        super.testGetTTLFromUnsupportedType();
    }

    @Override
    public void testSettingTTLOnNonStaticVertexLabel() {
        super.testSettingTTLOnNonStaticVertexLabel();
    }

    @Override
    public void testEdgeTTLImplicitKey() throws Exception {
        super.testEdgeTTLImplicitKey();
    }

    @Override
    public void testVertexTTLImplicitKey() throws Exception {
        super.testVertexTTLImplicitKey();
    }

    @Override
    public void testAutoSchemaMakerForVertexPropertyDataType() {
        super.testAutoSchemaMakerForVertexPropertyDataType();
    }

    @Override
    public void testAutoSchemaMakerForEdgePropertyDataType() {
        super.testAutoSchemaMakerForEdgePropertyDataType();
    }

    @Override
    public void testWriteAndReadWithJanusGraphIoRegistryWithGryo(Path tempDir) {
        super.testWriteAndReadWithJanusGraphIoRegistryWithGryo(tempDir);
    }

    @Override
    public void testWriteAndReadWithJanusGraphIoRegistryWithGraphson(Path tempDir) {
        super.testWriteAndReadWithJanusGraphIoRegistryWithGraphson(tempDir);
    }

    @Override
    public void testGetMatchingIndexes() {
        super.testGetMatchingIndexes();
    }

    @Override
    public void testExistsMatchingIndex() {
        super.testExistsMatchingIndex();
    }

    @Override
    public void testReindexingForEdgeIndex() throws InterruptedException, ExecutionException {
        super.testReindexingForEdgeIndex();
    }

    @Override
    public void testMultipleOrClauses() {
        super.testMultipleOrClauses();
    }

    @Override
    public void testMultipleNestedOrClauses() {
        super.testMultipleNestedOrClauses();
    }

    @Override
    public void testVerticesDropAfterWhereWithBatchQueryEnabled() {
        super.testVerticesDropAfterWhereWithBatchQueryEnabled();
    }
}
