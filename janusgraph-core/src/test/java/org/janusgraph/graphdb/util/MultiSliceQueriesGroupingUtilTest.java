// Copyright 2023 JanusGraph Authors
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

package org.janusgraph.graphdb.util;

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.tuple.Pair;
import org.janusgraph.diskstorage.keycolumnvalue.KeysQueriesGroup;
import org.janusgraph.diskstorage.keycolumnvalue.MultiKeysQueryGroups;
import org.janusgraph.diskstorage.keycolumnvalue.MultiQueriesByKeysGroupsContext;
import org.janusgraph.diskstorage.keycolumnvalue.SliceQuery;
import org.janusgraph.graphdb.internal.InternalVertex;
import org.janusgraph.graphdb.query.BackendQueryHolder;
import org.janusgraph.graphdb.vertices.CacheVertex;
import org.janusgraph.graphdb.vertices.StandardVertex;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static org.janusgraph.graphdb.util.MultiSliceQueriesGroupingUtil.DataTreeNode;
import static org.janusgraph.graphdb.util.MultiSliceQueriesGroupingUtil.TreeNode;
import static org.janusgraph.graphdb.util.MultiSliceQueriesGroupingUtil.moveQueriesToNewLeafNode;
import static org.janusgraph.graphdb.util.MultiSliceQueriesGroupingUtil.replaceCurrentLeafNodeWithUpdatedTypeLeafNodes;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

public class MultiSliceQueriesGroupingUtilTest {

    private static final AtomicLong VERTEX_ID = new AtomicLong(1);

    ///// Grouping testing

    @Test
    public void testGroupingNoVertices() {

        MultiKeysQueryGroups<Object, SliceQuery> result = MultiSliceQueriesGroupingUtil.toMultiKeysQueryGroups(Collections.emptyList(),
            Collections.singletonList(new BackendQueryHolder<>(mock(SliceQuery.class), false, false)));

        assertEquals(0, result.getQueryGroups().size());
        assertEquals(0, result.getMultiQueryContext().getAllKeysArr().length);
        assertEquals(0, result.getMultiQueryContext().getTotalAmountOfQueries());
        assertEquals(0, result.getMultiQueryContext().getAllLeafParents().size());
        assertContextIsValid(result.getMultiQueryContext());
    }

    @Test
    public void testGroupingNoQueries() {
        MultiKeysQueryGroups<Object, SliceQuery> result = MultiSliceQueriesGroupingUtil.toMultiKeysQueryGroups(
            Collections.singletonList(alwaysUseVertex()),
            Collections.emptyList());

        assertEquals(0, result.getQueryGroups().size());
        assertEquals(0, result.getMultiQueryContext().getAllKeysArr().length);
        assertEquals(0, result.getMultiQueryContext().getTotalAmountOfQueries());
        assertEquals(0, result.getMultiQueryContext().getAllLeafParents().size());
        assertContextIsValid(result.getMultiQueryContext());

        result = MultiSliceQueriesGroupingUtil.toMultiKeysQueryGroups(
            Collections.emptyList(),
            Collections.emptyList());

        assertEquals(0, result.getQueryGroups().size());
        assertEquals(0, result.getMultiQueryContext().getAllKeysArr().length);
        assertEquals(0, result.getMultiQueryContext().getTotalAmountOfQueries());
        assertEquals(0, result.getMultiQueryContext().getAllLeafParents().size());
        assertContextIsValid(result.getMultiQueryContext());
    }

    @Test
    public void testGroupingNonCacheableVertices() {
        MultiKeysQueryGroups<Object, SliceQuery> result = MultiSliceQueriesGroupingUtil.toMultiKeysQueryGroups(
            Collections.singletonList(nonCacheVertex()),
            Collections.singletonList(new BackendQueryHolder<>(mock(SliceQuery.class), false, false)));
        assertEquals(0, result.getQueryGroups().size());
        assertEquals(0, result.getMultiQueryContext().getAllKeysArr().length);
        assertEquals(0, result.getMultiQueryContext().getTotalAmountOfQueries());
        assertEquals(0, result.getMultiQueryContext().getAllLeafParents().size());
        assertContextIsValid(result.getMultiQueryContext());

        result = MultiSliceQueriesGroupingUtil.toMultiKeysQueryGroups(
            Arrays.asList(nonCacheVertex(), alwaysUseVertex(), nonCacheVertex()),
            Collections.singletonList(new BackendQueryHolder<>(mock(SliceQuery.class), false, false)));
        assertEquals(1, result.getQueryGroups().size());
        assertEquals(1, result.getQueryGroups().get(0).getQueries().size());
        assertEquals(1, result.getQueryGroups().get(0).getKeysGroup().size());
        assertEquals(1, result.getMultiQueryContext().getAllKeysArr().length);
        assertEquals(1, result.getMultiQueryContext().getTotalAmountOfQueries());
        assertEquals(1, result.getMultiQueryContext().getAllLeafParents().size());
        assertContextIsValid(result.getMultiQueryContext());
    }

    @Test
    public void testGroupingAlwaysSkipVertex() {
        MultiKeysQueryGroups<Object, SliceQuery> result = MultiSliceQueriesGroupingUtil.toMultiKeysQueryGroups(
            Collections.singletonList(alwaysSkipVertex()),
            Collections.singletonList(new BackendQueryHolder<>(mock(SliceQuery.class), false, false)));
        assertEquals(0, result.getQueryGroups().size());
        assertEquals(1, result.getMultiQueryContext().getAllKeysArr().length);
        assertEquals(1, result.getMultiQueryContext().getTotalAmountOfQueries());
        assertEquals(1, result.getMultiQueryContext().getAllLeafParents().size());
        assertContextIsValid(result.getMultiQueryContext());

        result = MultiSliceQueriesGroupingUtil.toMultiKeysQueryGroups(
            Arrays.asList(alwaysSkipVertex(), alwaysUseVertex(), nonCacheVertex()),
            Collections.singletonList(new BackendQueryHolder<>(mock(SliceQuery.class), false, false)));
        assertEquals(1, result.getQueryGroups().size());
        assertEquals(1, result.getQueryGroups().get(0).getQueries().size());
        assertEquals(1, result.getQueryGroups().get(0).getKeysGroup().size());
        assertEquals(2, result.getMultiQueryContext().getAllKeysArr().length);
        assertEquals(1, result.getMultiQueryContext().getTotalAmountOfQueries());
        assertEquals(1, result.getMultiQueryContext().getAllLeafParents().size());
        assertContextIsValid(result.getMultiQueryContext());
    }

    @Test
    public void testGroupingByQueries() {

        ArrayList<BackendQueryHolder<SliceQuery>> queries = new ArrayList<>();
        for (int i=0; i<10; i++){
            queries.add(new BackendQueryHolder<>(mock(SliceQuery.class), false, false));
        }

        ArrayList<InternalVertex> alwaysUseVertices = new ArrayList<>();
        for(int i=0;i<10;i++){
            alwaysUseVertices.add(alwaysUseVertex());
        }
        InternalVertex vertex1Query1 = vertex(queries.get(1).getBackendQuery());

        InternalVertex vertex1Query2 = vertex(queries.get(2).getBackendQuery(), queries.get(3).getBackendQuery());
        InternalVertex vertex2Query2 = vertex(queries.get(2).getBackendQuery(), queries.get(3).getBackendQuery());
        InternalVertex vertex3Query2 = vertex(queries.get(2).getBackendQuery(), queries.get(3).getBackendQuery());

        InternalVertex vertex1Query5_9 = vertex(queries.get(5).getBackendQuery(), queries.get(9).getBackendQuery());
        InternalVertex vertex1Query8 = vertex(queries.get(8).getBackendQuery());

        InternalVertex vertex1Query7 = vertex(queries.get(7).getBackendQuery());
        InternalVertex vertex2Query7 = vertex(queries.get(7).getBackendQuery());

        InternalVertex vertex1Query1_2_4_7 = vertex(queries.get(1).getBackendQuery(), queries.get(2).getBackendQuery(),
            queries.get(3).getBackendQuery(), queries.get(7).getBackendQuery());
        InternalVertex vertex2Query1_2_4_7 = vertex(queries.get(1).getBackendQuery(), queries.get(2).getBackendQuery(),
            queries.get(3).getBackendQuery(), queries.get(7).getBackendQuery());

        ArrayList<InternalVertex> allVertices = new ArrayList<>();
        allVertices.addAll(alwaysUseVertices);
        allVertices.add(alwaysSkipVertex());
        allVertices.add(alwaysSkipVertex());
        allVertices.add(alwaysSkipVertex());
        allVertices.add(vertex1Query1);
        allVertices.add(vertex1Query2);
        allVertices.add(vertex2Query2);
        allVertices.add(vertex3Query2);
        allVertices.add(vertex1Query5_9);
        allVertices.add(vertex1Query8);
        allVertices.add(vertex1Query7);
        allVertices.add(vertex2Query7);
        allVertices.add(vertex1Query1_2_4_7);
        allVertices.add(vertex2Query1_2_4_7);

        MultiKeysQueryGroups<Object, SliceQuery> result = MultiSliceQueriesGroupingUtil.toMultiKeysQueryGroups(
            allVertices, queries);

        assertEquals(6, result.getQueryGroups().size());
        assertEquals(alwaysUseVertices.size()+13, result.getMultiQueryContext().getAllKeysArr().length);
        assertEquals(queries.size(), result.getMultiQueryContext().getTotalAmountOfQueries());
        assertEquals(6, result.getMultiQueryContext().getAllLeafParents().size());
        assertContextIsValid(result.getMultiQueryContext());
        assertGroupedByUniqueQueriesAndVertexSets(result);

        Map<SliceQuery, Integer> queryVerticesSizes = new HashMap<>();
        for(Integer queryNumber : Arrays.asList(0, 4, 6)){
            queryVerticesSizes.put(queries.get(queryNumber).getBackendQuery(), alwaysUseVertices.size());
        }
        queryVerticesSizes.put(queries.get(1).getBackendQuery(), alwaysUseVertices.size() + 3);
        queryVerticesSizes.put(queries.get(2).getBackendQuery(), alwaysUseVertices.size() + 5);
        queryVerticesSizes.put(queries.get(3).getBackendQuery(), alwaysUseVertices.size() + 5);
        queryVerticesSizes.put(queries.get(5).getBackendQuery(), alwaysUseVertices.size() + 1);
        queryVerticesSizes.put(queries.get(7).getBackendQuery(), alwaysUseVertices.size() + 4);
        queryVerticesSizes.put(queries.get(8).getBackendQuery(), alwaysUseVertices.size() + 1);
        queryVerticesSizes.put(queries.get(9).getBackendQuery(), alwaysUseVertices.size() + 1);

        assertQueriesVertexSizes(result, queryVerticesSizes);

        result.getQueryGroups().forEach(group -> {
            if(group.getQueries().contains(queries.get(0).getBackendQuery())){
                assertEquals(3, group.getQueries().size());
                assertTrue(group.getQueries().contains(queries.get(4).getBackendQuery()));
                assertTrue(group.getQueries().contains(queries.get(6).getBackendQuery()));
            }
            if(group.getQueries().contains(queries.get(1).getBackendQuery())){
                assertEquals(1, group.getQueries().size());
            }
            if(group.getQueries().contains(queries.get(2).getBackendQuery())){
                assertEquals(2, group.getQueries().size());
                assertTrue(group.getQueries().contains(queries.get(3).getBackendQuery()));
            }
            if(group.getQueries().contains(queries.get(5).getBackendQuery())){
                assertEquals(2, group.getQueries().size());
                assertTrue(group.getQueries().contains(queries.get(9).getBackendQuery()));
            }
            if(group.getQueries().contains(queries.get(7).getBackendQuery())){
                assertEquals(1, group.getQueries().size());
            }
            if(group.getQueries().contains(queries.get(8).getBackendQuery())){
                assertEquals(1, group.getQueries().size());
            }
        });
    }

    @Test
    public void testGroupingByQueriesTwoElementsDifference() {
        // this module doesn't have JUnit platform, thus, we are not using @ParametrizedTest here.
        testGroupingByQueriesTwoElementsDifference(true);
        testGroupingByQueriesTwoElementsDifference(false);
    }

    private void testGroupingByQueriesTwoElementsDifference(boolean useFirst){
        ArrayList<BackendQueryHolder<SliceQuery>> queries = new ArrayList<>();
        queries.add(new BackendQueryHolder<>(mock(SliceQuery.class), false, false));
        queries.add(new BackendQueryHolder<>(mock(SliceQuery.class), false, false));

        ArrayList<InternalVertex> allVertices = new ArrayList<>();
        InternalVertex vertex1 = null;
        InternalVertex vertex2 = null;

        if(useFirst){
            vertex1 = vertex(queries.get(0).getBackendQuery());
            vertex2 = vertex(queries.get(1).getBackendQuery());
            allVertices.add(vertex1);
            allVertices.add(vertex2);
        }
        for(int i=0;i<10;i++){
            allVertices.add(alwaysUseVertex());
        }
        if(!useFirst){
            vertex1 = vertex(queries.get(0).getBackendQuery());
            vertex2 = vertex(queries.get(1).getBackendQuery());
            allVertices.add(vertex1);
            allVertices.add(vertex2);
        }

        MultiKeysQueryGroups<Object, SliceQuery> result = MultiSliceQueriesGroupingUtil.toMultiKeysQueryGroups(
            allVertices, queries);

        assertEquals(2, result.getQueryGroups().size());
        assertEquals(allVertices.size(), result.getMultiQueryContext().getAllKeysArr().length);
        assertEquals(queries.size(), result.getMultiQueryContext().getTotalAmountOfQueries());
        assertEquals(2, result.getMultiQueryContext().getAllLeafParents().size());
        assertContextIsValid(result.getMultiQueryContext());
        assertGroupedByUniqueQueriesAndVertexSets(result);

        Map<SliceQuery, Integer> queryVerticesSizes = new HashMap<>();
        queryVerticesSizes.put(queries.get(0).getBackendQuery(), allVertices.size() - 1);
        queryVerticesSizes.put(queries.get(1).getBackendQuery(), allVertices.size() - 1);
        assertQueriesVertexSizes(result, queryVerticesSizes);

        for(KeysQueriesGroup<Object, SliceQuery> group : result.getQueryGroups()){
            if(group.getQueries().contains(queries.get(0).getBackendQuery())){
                assertEquals(1, group.getQueries().size());
                assertTrue(group.getKeysGroup().contains(vertex1.id()));
                assertFalse(group.getKeysGroup().contains(vertex2.id()));
            }
            if(group.getQueries().contains(queries.get(1).getBackendQuery())){
                assertEquals(1, group.getQueries().size());
                assertTrue(group.getKeysGroup().contains(vertex2.id()));
                assertFalse(group.getKeysGroup().contains(vertex1.id()));
            }
        }
    }

    @Test
    public void testGroupingByQueriesLastOneElementDifference(){
        ArrayList<BackendQueryHolder<SliceQuery>> queries = new ArrayList<>();
        queries.add(new BackendQueryHolder<>(mock(SliceQuery.class), false, false));
        queries.add(new BackendQueryHolder<>(mock(SliceQuery.class), false, false));

        ArrayList<InternalVertex> allVertices = new ArrayList<>();

        for(int i=0;i<10;i++){
            allVertices.add(alwaysUseVertex());
        }
        InternalVertex vertex1 = vertex(queries.get(0).getBackendQuery());
        allVertices.add(vertex1);

        MultiKeysQueryGroups<Object, SliceQuery> result = MultiSliceQueriesGroupingUtil.toMultiKeysQueryGroups(
            allVertices, queries);

        assertEquals(2, result.getQueryGroups().size());
        assertEquals(allVertices.size(), result.getMultiQueryContext().getAllKeysArr().length);
        assertEquals(queries.size(), result.getMultiQueryContext().getTotalAmountOfQueries());
        assertEquals(1, result.getMultiQueryContext().getAllLeafParents().size());
        assertContextIsValid(result.getMultiQueryContext());
        assertGroupedByUniqueQueriesAndVertexSets(result);

        Map<SliceQuery, Integer> queryVerticesSizes = new HashMap<>();
        queryVerticesSizes.put(queries.get(0).getBackendQuery(), allVertices.size());
        queryVerticesSizes.put(queries.get(1).getBackendQuery(), allVertices.size() - 1);
        assertQueriesVertexSizes(result, queryVerticesSizes);

        for(KeysQueriesGroup<Object, SliceQuery> group : result.getQueryGroups()){
            if(group.getQueries().contains(queries.get(0).getBackendQuery())){
                assertEquals(1, group.getQueries().size());
                assertTrue(group.getKeysGroup().contains(vertex1.id()));
                assertEquals(allVertices.size(), group.getKeysGroup().size());
            }
            if(group.getQueries().contains(queries.get(1).getBackendQuery())){
                assertEquals(1, group.getQueries().size());
                assertFalse(group.getKeysGroup().contains(vertex1.id()));
                assertEquals(allVertices.size()-1, group.getKeysGroup().size());
            }
        }
    }

    @Test
    public void testGroupingVertexMultipleQueries() {
        MultiKeysQueryGroups<Object, SliceQuery> result = MultiSliceQueriesGroupingUtil.toMultiKeysQueryGroups(
            Collections.singletonList(alwaysUseVertex()),
            Arrays.asList(new BackendQueryHolder<>(mock(SliceQuery.class), false, false),
                new BackendQueryHolder<>(mock(SliceQuery.class), false, false),
                new BackendQueryHolder<>(mock(SliceQuery.class), false, false)));
        assertEquals(1, result.getQueryGroups().size());
        assertEquals(3, result.getQueryGroups().get(0).getQueries().size());
        assertEquals(1, result.getQueryGroups().get(0).getKeysGroup().size());
        assertEquals(1, result.getMultiQueryContext().getAllKeysArr().length);
        assertEquals(3, result.getMultiQueryContext().getTotalAmountOfQueries());
        assertEquals(1, result.getMultiQueryContext().getAllLeafParents().size());
        assertContextIsValid(result.getMultiQueryContext());
    }

    ///// Nodes replacement testing

    @Test
    public void testReplaceCurrentLeafNodeWithUpdatedTypeLeafNodesEmptyList() {
        List<TreeNode> allLeafParents = Collections.emptyList();
        Map<Object, Object> oldToNewKeysMap = new HashMap<>();
        MultiSliceQueriesGroupingUtil.replaceCurrentLeafNodeWithUpdatedTypeLeafNodes(allLeafParents, oldToNewKeysMap);
        assertTrue(allLeafParents.isEmpty());
        assertTrue(oldToNewKeysMap.isEmpty());
    }

    @Test
    public void testReplaceCurrentLeafNodeWithUpdatedTypeLeafNodeBothChildrenDataTreeNode() {
        // Create leaf nodes with both children being instances of DataTreeNode
        DataTreeNode<KeysQueriesGroup<Integer, SliceQuery>> leftChild = new DataTreeNode<>(new KeysQueriesGroup<>(Collections.singletonList(1), Collections.emptyList()));
        DataTreeNode<KeysQueriesGroup<Integer, SliceQuery>> rightChild = new DataTreeNode<>(new KeysQueriesGroup<>(Collections.singletonList(2), Collections.emptyList()));
        TreeNode parentNode = new TreeNode(leftChild, rightChild);
        List<TreeNode> allLeafParents = Collections.singletonList(parentNode);

        // Create a mapping from Integer to String
        Map<Integer, String> oldToNewKeysMap = new HashMap<>();
        oldToNewKeysMap.put(1, "one");
        oldToNewKeysMap.put(2, "two");

        replaceCurrentLeafNodeWithUpdatedTypeLeafNodes(allLeafParents, oldToNewKeysMap);

        // Verify that both children are replaced with updated type leaf nodes
        assertTrue(parentNode.left instanceof DataTreeNode);
        assertTrue(parentNode.right instanceof DataTreeNode);
        assertEquals("one", ((DataTreeNode<KeysQueriesGroup>) parentNode.left).data.getKeysGroup().get(0));
        assertEquals("two", ((DataTreeNode<KeysQueriesGroup>) parentNode.right).data.getKeysGroup().get(0));
    }

    @Test
    public void testReplaceCurrentLeafNodeWithUpdatedTypeLeafNodesOnlyLeftChildDataTreeNode() {
        // Create a leaf node with only the left child being an instance of DataTreeNode
        DataTreeNode<KeysQueriesGroup<Integer, SliceQuery>> leftChild = new DataTreeNode<>(new KeysQueriesGroup<>(Collections.singletonList(1), Collections.emptyList()));
        TreeNode parentNode = new TreeNode(leftChild, new TreeNode());
        List<TreeNode> allLeafParents = Collections.singletonList(parentNode);

        // Create a mapping from Integer to String
        Map<Integer, String> oldToNewKeysMap = new HashMap<>();
        oldToNewKeysMap.put(1, "one");

        replaceCurrentLeafNodeWithUpdatedTypeLeafNodes(allLeafParents, oldToNewKeysMap);

        // Verify that only the left child is replaced with an updated type leaf node
        assertTrue(parentNode.left instanceof DataTreeNode);
        assertFalse(parentNode.right instanceof DataTreeNode);
        assertEquals("one", ((DataTreeNode<KeysQueriesGroup>) parentNode.left).data.getKeysGroup().get(0));
    }

    @Test
    public void testReplaceCurrentLeafNodeWithUpdatedTypeLeafNodesOnlyRightChildDataTreeNode() {
        // Create a leaf node with only the right child being an instance of DataTreeNode
        TreeNode parentNode = new TreeNode(new TreeNode(), new DataTreeNode<>(new KeysQueriesGroup<>(Collections.singletonList(2), Collections.emptyList())));
        List<TreeNode> allLeafParents = Collections.singletonList(parentNode);

        // Create a mapping from Integer to String
        Map<Integer, String> oldToNewKeysMap = new HashMap<>();
        oldToNewKeysMap.put(2, "two");

        replaceCurrentLeafNodeWithUpdatedTypeLeafNodes(allLeafParents, oldToNewKeysMap);

        // Verify that only the right child is replaced with an updated type leaf node
        assertFalse(parentNode.left instanceof DataTreeNode);
        assertTrue(parentNode.right instanceof DataTreeNode);
        assertEquals("two", ((DataTreeNode<KeysQueriesGroup>) parentNode.right).data.getKeysGroup().get(0));
    }

    @Test
    public void testReplaceCurrentLeafNodeWithUpdatedTypeLeafNodesNoDataTreeNodeChildren() {
        // Create leaf nodes with both children not being instances of DataTreeNode
        TreeNode parentNode = new TreeNode(new TreeNode(), new TreeNode());
        List<TreeNode> allLeafParents = Collections.singletonList(parentNode);

        // Create a mapping from Integer to String
        Map<Integer, String> oldToNewKeysMap = new HashMap<>();
        oldToNewKeysMap.put(1, "one");
        oldToNewKeysMap.put(2, "two");

        replaceCurrentLeafNodeWithUpdatedTypeLeafNodes(allLeafParents, oldToNewKeysMap);

        // Verify that no modifications occur
        assertFalse(parentNode.left instanceof DataTreeNode);
        assertFalse(parentNode.right instanceof DataTreeNode);
    }

    @Test
    public void testReplaceCurrentLeafNodeWithUpdatedTypeLeafNodesEmptyMap() {
        // Create leaf nodes with both children being instances of DataTreeNode
        DataTreeNode<KeysQueriesGroup<Integer, SliceQuery>> leftChild = new DataTreeNode<>(new KeysQueriesGroup<>(Collections.singletonList(1), Collections.emptyList()));
        DataTreeNode<KeysQueriesGroup<Integer, SliceQuery>> rightChild = new DataTreeNode<>(new KeysQueriesGroup<>(Collections.singletonList(2), Collections.emptyList()));
        TreeNode parentNode = new TreeNode(leftChild, rightChild);
        List<TreeNode> allLeafParents = Collections.singletonList(parentNode);

        // Empty mapping
        Map<Integer, String> oldToNewKeysMap = Collections.emptyMap();

        replaceCurrentLeafNodeWithUpdatedTypeLeafNodes(allLeafParents, oldToNewKeysMap);

        // Verify that no modifications occur
        assertTrue(parentNode.left instanceof DataTreeNode);
        assertTrue(parentNode.right instanceof DataTreeNode);
        assertNull(((DataTreeNode<KeysQueriesGroup>) parentNode.left).data.getKeysGroup().get(0));
        assertNull(((DataTreeNode<KeysQueriesGroup>) parentNode.right).data.getKeysGroup().get(0));
    }

    @Test
    public void testReplaceCurrentLeafNodeWithUpdatedTypeLeafNodesKeysNotPresentInData() {
        // Create leaf nodes with both children being instances of DataTreeNode
        DataTreeNode<KeysQueriesGroup<Integer, SliceQuery>> leftChild = new DataTreeNode<>(new KeysQueriesGroup<>(Collections.singletonList(1), Collections.emptyList()));
        DataTreeNode<KeysQueriesGroup<Integer, SliceQuery>> rightChild = new DataTreeNode<>(new KeysQueriesGroup<>(Collections.singletonList(2), Collections.emptyList()));
        TreeNode parentNode = new TreeNode(leftChild, rightChild);
        List<TreeNode> allLeafParents = Collections.singletonList(parentNode);

        // Create a mapping from Integer to String
        Map<Integer, String> oldToNewKeysMap = new HashMap<>();
        oldToNewKeysMap.put(3, "three");

        replaceCurrentLeafNodeWithUpdatedTypeLeafNodes(allLeafParents, oldToNewKeysMap);

        // Verify that no modifications occur
        assertTrue(parentNode.left instanceof DataTreeNode);
        assertTrue(parentNode.right instanceof DataTreeNode);
        assertNull(((DataTreeNode<KeysQueriesGroup>) parentNode.left).data.getKeysGroup().get(0));
        assertNull(((DataTreeNode<KeysQueriesGroup>) parentNode.right).data.getKeysGroup().get(0));
    }

    @Test
    public void testReplaceCurrentLeafNodeWithUpdatedTypeLeafNodesValidMapping() {
        // Create leaf nodes with both children being instances of DataTreeNode
        DataTreeNode<KeysQueriesGroup<Integer, SliceQuery>> leftChild = new DataTreeNode<>(new KeysQueriesGroup<>(Collections.singletonList(1), Collections.emptyList()));
        DataTreeNode<KeysQueriesGroup<Integer, SliceQuery>> rightChild = new DataTreeNode<>(new KeysQueriesGroup<>(Collections.singletonList(2), Collections.emptyList()));
        TreeNode parentNode = new TreeNode(leftChild, rightChild);
        List<TreeNode> allLeafParents = Collections.singletonList(parentNode);

        // Create a mapping from Integer to String
        Map<Integer, String> oldToNewKeysMap = new HashMap<>();
        oldToNewKeysMap.put(1, "one");
        oldToNewKeysMap.put(2, "two");

        replaceCurrentLeafNodeWithUpdatedTypeLeafNodes(allLeafParents, oldToNewKeysMap);

        // Verify that both children are replaced with updated type leaf nodes
        assertTrue(parentNode.left instanceof DataTreeNode);
        assertTrue(parentNode.right instanceof DataTreeNode);
        assertEquals("one", ((DataTreeNode<KeysQueriesGroup>) parentNode.left).data.getKeysGroup().get(0));
        assertEquals("two", ((DataTreeNode<KeysQueriesGroup>) parentNode.right).data.getKeysGroup().get(0));
    }

    // Queries movement testing

    @Test
    public void testMoveQueriesToNewLeafNodeEmptyUpdatedQueryGroup() {
        List<Pair<SliceQuery, List<Integer>>> updatedQueryGroup = Collections.emptyList();
        Integer[] allVertices = {1, 2, 3};
        TreeNode groupingRootTreeNode = new TreeNode(null, new TreeNode());
        List<KeysQueriesGroup<Integer, SliceQuery>> remainingQueryGroups = new ArrayList<>();

        moveQueriesToNewLeafNode(updatedQueryGroup, allVertices, groupingRootTreeNode, remainingQueryGroups);

        assertNull(groupingRootTreeNode.left);
        assertNull(groupingRootTreeNode.right.right);
        assertNull(groupingRootTreeNode.right.left);
        assertTrue(remainingQueryGroups.isEmpty());
    }

    @Test
    public void testMoveQueriesToNewLeafNodeMatchingVertices() {
        // Create leaf nodes with matching vertices
        TreeNode leftChild = new TreeNode();
        TreeNode rightChild = new TreeNode();
        TreeNode groupingRootTreeNode = new TreeNode(leftChild, rightChild);
        List<Pair<SliceQuery, List<Integer>>> updatedQueryGroup = new ArrayList<>();
        updatedQueryGroup.add(Pair.of(mock(SliceQuery.class), Arrays.asList(1, 2)));
        updatedQueryGroup.add(Pair.of(mock(SliceQuery.class), Collections.singletonList(3)));
        Integer[] allVertices = {1, 2, 3};
        List<KeysQueriesGroup<Integer, SliceQuery>> remainingQueryGroups = new ArrayList<>();

       moveQueriesToNewLeafNode(updatedQueryGroup, allVertices, groupingRootTreeNode, remainingQueryGroups);

        // Verify that the queries are added to the corresponding leaf nodes
        assertEquals(1, ((DataTreeNode<KeysQueriesGroup>) leftChild.left.right).data.getQueries().size());
        assertEquals(1, ((DataTreeNode<KeysQueriesGroup>) rightChild.right.left).data.getQueries().size());
        assertEquals(2, remainingQueryGroups.size());
    }

    @Test
    public void testMoveQueriesToNewLeafNodeNewChainNodes() {
        // Create a leaf node without matching vertices
        List<Pair<SliceQuery, List<Integer>>> updatedQueryGroup = new ArrayList<>();
        updatedQueryGroup.add(Pair.of(mock(SliceQuery.class), Collections.singletonList(1)));
        updatedQueryGroup.add(Pair.of(mock(SliceQuery.class), Collections.singletonList(2)));
        updatedQueryGroup.add(Pair.of(mock(SliceQuery.class), Collections.singletonList(3)));
        updatedQueryGroup.add(Pair.of(mock(SliceQuery.class), Arrays.asList(1,2,3)));

        Integer[] allVertices = {1, 2, 3};
        List<KeysQueriesGroup<Integer, SliceQuery>> remainingQueryGroups = new ArrayList<>();
        KeysQueriesGroup<Integer, SliceQuery> existingQueryData = new KeysQueriesGroup<>(Arrays.asList(allVertices), new ArrayList<>());
        remainingQueryGroups.add(existingQueryData);
        TreeNode root = new TreeNode(null, new TreeNode(null, new TreeNode(null, new DataTreeNode<>(existingQueryData))));

       moveQueriesToNewLeafNode(updatedQueryGroup, allVertices, root, remainingQueryGroups);

        // Verify that new chain nodes are generated and queries are added to the corresponding leaf nodes
        assertEquals(3, ((DataTreeNode<KeysQueriesGroup<Integer, SliceQuery>>) root.right.right.right).data.getKeysGroup().size());
        assertTrue(((DataTreeNode<KeysQueriesGroup<Integer, SliceQuery>>) root.right.right.right).data.getKeysGroup().containsAll(Arrays.asList(allVertices)));
        assertEquals(1, ((DataTreeNode<KeysQueriesGroup<Integer, SliceQuery>>) root.right.right.right).data.getQueries().size());
        assertEquals(updatedQueryGroup.get(3).getKey(), ((DataTreeNode<KeysQueriesGroup<Integer, SliceQuery>>) root.right.right.right).data.getQueries().iterator().next());
        assertEquals(existingQueryData, ((DataTreeNode<KeysQueriesGroup<Integer, SliceQuery>>) root.right.right.right).data);

        assertEquals(1, ((DataTreeNode<KeysQueriesGroup<Integer, SliceQuery>>) root.right.left.left).data.getQueries().size());
        assertEquals(1, ((DataTreeNode<KeysQueriesGroup<Integer, SliceQuery>>) root.left.right.left).data.getQueries().size());
        assertEquals(1, ((DataTreeNode<KeysQueriesGroup<Integer, SliceQuery>>) root.left.left.right).data.getQueries().size());
        assertEquals(updatedQueryGroup.get(0).getKey(), ((DataTreeNode<KeysQueriesGroup<Integer, SliceQuery>>) root.right.left.left).data.getQueries().iterator().next());
        assertEquals(updatedQueryGroup.get(1).getKey(), ((DataTreeNode<KeysQueriesGroup<Integer, SliceQuery>>) root.left.right.left).data.getQueries().iterator().next());
        assertEquals(updatedQueryGroup.get(2).getKey(), ((DataTreeNode<KeysQueriesGroup<Integer, SliceQuery>>) root.left.left.right).data.getQueries().iterator().next());
        assertNull(root.right.right.left);
        assertNull(root.right.left.right);
        assertNull(root.left.right.right);
        assertNull(root.left.left.left);

        assertEquals(4, remainingQueryGroups.size());
        assertTrue(remainingQueryGroups.containsAll(Arrays.asList(
            ((DataTreeNode<KeysQueriesGroup<Integer, SliceQuery>>) root.right.right.right).data,
            ((DataTreeNode<KeysQueriesGroup<Integer, SliceQuery>>) root.right.left.left).data,
            ((DataTreeNode<KeysQueriesGroup<Integer, SliceQuery>>) root.left.right.left).data,
            ((DataTreeNode<KeysQueriesGroup<Integer, SliceQuery>>) root.left.left.right).data
        )));
    }

    @Test
    public void testMoveQueriesToNewLeafNodeEmptyRemainingQueryGroups() {
        TreeNode leaf1 = new TreeNode();
        TreeNode leaf2 = new TreeNode();
        TreeNode groupingRootTreeNode = new TreeNode(leaf1, leaf2);
        List<Pair<SliceQuery, List<Integer>>> updatedQueryGroup = new ArrayList<>();
        updatedQueryGroup.add(Pair.of(mock(SliceQuery.class), Arrays.asList(1, 2)));
        Integer[] allVertices = {1, 2, 3};
        List<KeysQueriesGroup<Integer, SliceQuery>> remainingQueryGroups = new ArrayList<>();

        moveQueriesToNewLeafNode(updatedQueryGroup, allVertices, groupingRootTreeNode, remainingQueryGroups);

        assertEquals(1, remainingQueryGroups.size());
    }

    @Test
    public void testMoveQueriesToNewLeafNodeNonEmptyRemainingQueryGroups() {
        List<Pair<SliceQuery, List<Integer>>> updatedQueryGroup = new ArrayList<>();
        updatedQueryGroup.add(Pair.of(mock(SliceQuery.class), Collections.singletonList(1)));
        Integer[] allVertices = {1};
        List<KeysQueriesGroup<Integer, SliceQuery>> remainingQueryGroups = new ArrayList<>();
        remainingQueryGroups.add(new KeysQueriesGroup<>(Collections.emptyList(), Collections.emptyList()));
        TreeNode groupingRootTreeNode = new TreeNode(new DataTreeNode<>(remainingQueryGroups.get(0)), null);

        moveQueriesToNewLeafNode(updatedQueryGroup, allVertices, groupingRootTreeNode, remainingQueryGroups);

        assertEquals(2, remainingQueryGroups.size());
        assertEquals(1, remainingQueryGroups.get(1).getKeysGroup().size());
        assertTrue(remainingQueryGroups.get(1).getKeysGroup().contains(1));
        assertEquals(1, remainingQueryGroups.get(1).getQueries().size());
        assertTrue(remainingQueryGroups.get(1).getQueries().contains(updatedQueryGroup.get(0).getKey()));
    }

    private void assertContextIsValid(MultiQueriesByKeysGroupsContext<Object> context){
        assertTrue(context.getAllLeafParents().size() <= context.getTotalAmountOfQueries());

        Object[] keys = context.getAllKeysArr();
        Set<Object> keysSet = new HashSet<>(Arrays.asList(keys));
        assertEquals(keysSet.size(), keys.length);

        Set<Integer> allLeafDepths = new HashSet<>(1);
        addAllLeafDepths(context.getGroupingRootTreeNode(), 0, allLeafDepths);
        if(keysSet.isEmpty()){
            assertTrue(allLeafDepths.isEmpty());
        } else {
            assertEquals(1, allLeafDepths.size());
            Integer calculatedTreeDepth = allLeafDepths.iterator().next();
            assertEquals(keys.length, calculatedTreeDepth);
        }

        List<DataTreeNode> calculatedLeafNodes = new ArrayList<>();
        addAllLeafNodes(context.getGroupingRootTreeNode(), calculatedLeafNodes);
        assertTrue(calculatedLeafNodes.size() >= context.getAllLeafParents().size());

        for(DataTreeNode leafNode : calculatedLeafNodes){
            boolean childOfLeafParents = false;
            for(TreeNode parentNode : context.getAllLeafParents()){
                if(parentNode.left == leafNode || parentNode.right == leafNode){
                    assertFalse(childOfLeafParents, "This child node was already a child of another parent. " +
                        "Each parent should have unique children nodes.");
                    childOfLeafParents = true;
                }
            }
            assertTrue(childOfLeafParents, "Calculated leaf node is not a child of any of the context's leaf parent nodes.");
        }
    }

    private void addAllLeafDepths(TreeNode node, int currentDepth, Set<Integer> depths){
        if(node == null){
            return;
        }
        if(node instanceof DataTreeNode){
            assertNull(node.left);
            assertNull(node.right);
            depths.add(currentDepth);
            return;
        }
        addAllLeafDepths(node.left, currentDepth+1, depths);
        addAllLeafDepths(node.right, currentDepth+1, depths);
    }

    private void addAllLeafNodes(TreeNode node, List<DataTreeNode> leafNodes){
        if(node == null){
            return;
        }
        if(node instanceof DataTreeNode){
            assertNull(node.left);
            assertNull(node.right);
            leafNodes.add((DataTreeNode) node);
            return;
        }
        addAllLeafNodes(node.left, leafNodes);
        addAllLeafNodes(node.right, leafNodes);
    }

    private StandardVertex nonCacheVertex(){
        return mock(StandardVertex.class);
    }

    private CacheVertex vertex(SliceQuery ... queriesWithHasLoadedRelations){
        return mockVertex(CacheVertex.class, queriesWithHasLoadedRelations);
    }

    private CacheVertex alwaysSkipVertex(){
        return mockVertex(CacheVertex.class, true);
    }

    private CacheVertex alwaysUseVertex(){
        return mockVertex(CacheVertex.class, false);
    }

    private <T extends InternalVertex> T mockVertex(Class<T> type, boolean hasLoadedRelations){
        T vertex = mock(type);
        doReturn(hasLoadedRelations).when(vertex).hasLoadedRelations(any());
        doReturn(false).when(vertex).isNew();
        doReturn(true).when(vertex).hasId();
        doReturn(VERTEX_ID.incrementAndGet()).when(vertex).id();
        doReturn(hasLoadedRelations).when(vertex).hasLoadedRelations(any());
        return vertex;
    }

    private <T extends InternalVertex> T mockVertex(Class<T> type, SliceQuery ... queriesWithHasLoadedRelations){
        T vertex = mock(type);
        doReturn(false).when(vertex).isNew();
        doReturn(true).when(vertex).hasId();
        doReturn(VERTEX_ID.incrementAndGet()).when(vertex).id();
        Set<SliceQuery> falseQueries = new HashSet<>(Arrays.asList(queriesWithHasLoadedRelations));
        doAnswer(invocationOnMock -> {
            return !falseQueries.contains(invocationOnMock.getArgument(0));
        }).when(vertex).hasLoadedRelations(any());
        return vertex;
    }


    private void assertQueriesVertexSizes(MultiKeysQueryGroups<Object, SliceQuery> result, Map<SliceQuery, Integer> queryVerticesSizes){
        result.getQueryGroups().forEach(group -> group.getQueries().forEach(query -> assertEquals(queryVerticesSizes.get(query), group.getKeysGroup().size())));
    }

    private void assertGroupedByUniqueQueriesAndVertexSets(MultiKeysQueryGroups<Object, SliceQuery> result){

        // Check all slice queries are unique across all collections
        result.getQueryGroups().forEach(group -> {
            group.getQueries().forEach(query -> {
                MutableBoolean duplicate = new MutableBoolean(false);
                result.getQueryGroups().forEach(group2 -> {
                    group2.getQueries().forEach(query2 -> {
                        if(query == query2){
                            assertFalse(duplicate.booleanValue());
                            duplicate.setTrue();
                        }
                    });
                });
            });
        });

        // Check all vertex sets are unique across all pairs
        result.getQueryGroups().forEach(group -> {
            MutableBoolean duplicate = new MutableBoolean(false);
            result.getQueryGroups().forEach(group2 -> {
                if(group.getKeysGroup().size() == group2.getKeysGroup().size() && group.getKeysGroup().containsAll(group2.getKeysGroup())){
                    assertFalse(duplicate.booleanValue());
                    duplicate.setTrue();
                }
            });
        });
    }

}
