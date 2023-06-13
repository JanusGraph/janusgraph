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

import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.tuple.Pair;
import org.janusgraph.diskstorage.keycolumnvalue.KeysQueriesGroup;
import org.janusgraph.diskstorage.keycolumnvalue.MultiKeysQueryGroups;
import org.janusgraph.diskstorage.keycolumnvalue.MultiQueriesByKeysGroupsContext;
import org.janusgraph.diskstorage.keycolumnvalue.SliceQuery;
import org.janusgraph.graphdb.internal.InternalVertex;
import org.janusgraph.graphdb.query.BackendQueryHolder;
import org.janusgraph.graphdb.vertices.CacheVertex;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Utility class which helps to group necessary queries to matched vertices for the following multi-query execution.
 */
public class MultiSliceQueriesGroupingUtil {

    private static final MultiKeysQueryGroups<Object, SliceQuery> EMPTY_QUERY = new MultiKeysQueryGroups<>(Collections.emptyList(),
        new MultiQueriesByKeysGroupsContext<>(new Object[0], new TreeNode(), 0, Collections.emptyList()));

    /**
     * Moves queries `updatedQueryGroup` to either existing leaf nodes or generates new leaf nodes for the necessary key sets.
     *
     * @param updatedQueryGroup Query groups to which a query should be moved.
     * @param allVertices All vertices
     * @param groupingRootTreeNode Root tree node which represents chains where each left node represents a missing key
     *                             from `allVertices` array, and each right node represents existing key from `allVertices`
     *                             array.
     * @param remainingQueryGroups Remaining groups where queries from `updatedQueryGroup` should be added. Notice, if
     *                             a new leaf node is added into `groupingRootTreeNode` then a new group is added to
     *                             `remainingQueryGroups`.
     */
    public static <K> void moveQueriesToNewLeafNode(List<Pair<SliceQuery, List<K>>> updatedQueryGroup,
                                                    K[] allVertices,
                                                    TreeNode groupingRootTreeNode,
                                                    List<KeysQueriesGroup<K, SliceQuery>> remainingQueryGroups){

        for(Pair<SliceQuery, List<K>> keyNewQueriesGroup : updatedQueryGroup){
            boolean newChainGenerated = false;
            TreeNode previousNode = groupingRootTreeNode;
            TreeNode currentNode = groupingRootTreeNode;
            boolean rightNode = false;
            Iterator<K> usedKeyIt = keyNewQueriesGroup.getValue().iterator();
            for(int keyIndex = 0; keyIndex < allVertices.length; keyIndex++){

                K usedKey = usedKeyIt.hasNext() ? usedKeyIt.next() : null;

                while (keyIndex < allVertices.length && usedKey != allVertices[keyIndex]){
                    rightNode = false;
                    if(currentNode.left == null){
                        currentNode.left = new TreeNode();
                        newChainGenerated = true;
                    }
                    previousNode = currentNode;
                    currentNode = currentNode.left;
                    ++keyIndex;
                }

                if(keyIndex == allVertices.length){
                    break;
                }

                rightNode = true;
                if(currentNode.right == null){
                    currentNode.right = new TreeNode();
                    newChainGenerated = true;
                }
                previousNode = currentNode;
                currentNode = currentNode.right;
            }

            DataTreeNode<KeysQueriesGroup<K, SliceQuery>> chainLeafNode;
            if(newChainGenerated){
                KeysQueriesGroup<K, SliceQuery> data = new KeysQueriesGroup<>(keyNewQueriesGroup.getValue(), new ArrayList<>());
                chainLeafNode = new DataTreeNode<>(data);
                if(rightNode){
                    previousNode.right = chainLeafNode;
                } else {
                    previousNode.left = chainLeafNode;
                }
                remainingQueryGroups.add(data);
            } else {
                chainLeafNode = (DataTreeNode<KeysQueriesGroup<K, SliceQuery>>) currentNode;
            }
            chainLeafNode.data.getQueries().add(keyNewQueriesGroup.getKey());
        }
    }

    /**
     * Replaces child leaf nodes which have data {@code KeysQueriesGroup<C>} with new leaf nodes which have keys replaces
     * by `oldToNewKeysMap`. The resulting data of child nodes will be {@code KeysQueriesGroup<N>}.
     *
     * @param allLeafParents parent nodes
     * @param oldToNewKeysMap map to replace old type keys with new type keys
     * @param <C> current key type
     * @param <N> new key type
     */
    public static <C,N> void replaceCurrentLeafNodeWithUpdatedTypeLeafNodes(List<TreeNode> allLeafParents, Map<C, N> oldToNewKeysMap){
        for(TreeNode node : allLeafParents){
            if(node.left instanceof DataTreeNode){
                node.left = generateConvertedLeafNode((DataTreeNode<KeysQueriesGroup<C, SliceQuery>>) node.left, oldToNewKeysMap);
            }
            if(node.right instanceof DataTreeNode){
                node.right = generateConvertedLeafNode((DataTreeNode<KeysQueriesGroup<C, SliceQuery>>) node.right, oldToNewKeysMap);
            }
        }
    }

    private static <C,N> DataTreeNode<KeysQueriesGroup<N, SliceQuery>> generateConvertedLeafNode(DataTreeNode<KeysQueriesGroup<C, SliceQuery>> currentLeafNode, Map<C, N> oldToNewKeysMap){
        KeysQueriesGroup<C, SliceQuery> data = currentLeafNode.data;
        List<N> keysGroup = new ArrayList<>(data.getKeysGroup().size());
        for(C currentKey : data.getKeysGroup()){
            keysGroup.add(oldToNewKeysMap.get(currentKey));
        }
        KeysQueriesGroup<N, SliceQuery> newData = new KeysQueriesGroup<>(keysGroup, data.getQueries());
        return new DataTreeNode<>(newData);
    }

    /**
     * Queries grouping algorithm for the same sets of keys (vertices).
     * <br>
     * This algorithm uses a binary prefix tree to find a group with the same vertices (same vertices Set).
     * All and only the leaf nodes store the final computed data (queries used for this leaf node + vertices used for this leaf nodes).
     * Each leaf node represents a group of queries which always have the same vertices.
     * Time complexity is always O(N+M), where N is the vertices amount and M is the queries amount.
     * Space complexity in most cases is O(N+M), or O(N*M) in the worst case.
     *
     * @param vertices all vertices.
     * @param queries all queries which should be executed for all vertices.
     * @return Generated grouped multi-query representation.
     */
    public static MultiKeysQueryGroups<Object, SliceQuery> toMultiKeysQueryGroups(final Collection<InternalVertex> vertices, final List<BackendQueryHolder<SliceQuery>> queries){
        if(queries.isEmpty()){
            return EMPTY_QUERY;
        }
        MutableInt verticesMutableSize = new MutableInt();
        InternalVertex[] cacheableVertices = filterNonCacheableVertices(vertices, verticesMutableSize);
        final int verticesSize = verticesMutableSize.intValue();
        Object[] vertexIds = toIds(cacheableVertices, verticesSize);
        if(verticesSize == 0){
            return EMPTY_QUERY;
        }
        List<KeysQueriesGroup<Object, SliceQuery>> result = new ArrayList<>();
        TreeNode root = new TreeNode();
        boolean[] useVertex = new boolean[verticesSize];
        List<TreeNode> allLeafParents = new ArrayList<>();
        for(BackendQueryHolder<SliceQuery> queryHolder : queries){
            final SliceQuery query = queryHolder.getBackendQuery();
            TreeNode currentNode = root;
            int usedVertices = 0;
            for(int i=0; i<verticesSize; i++){
                if (cacheableVertices[i].hasLoadedRelations(query)){
                    useVertex[i] = false;
                    if(currentNode.left == null){
                        currentNode = generateNewChainAndReturnLeafNode(true, currentNode, cacheableVertices,
                            useVertex, i, usedVertices, verticesSize, query, result, allLeafParents);
                        break;
                    } else {
                        currentNode = currentNode.left;
                    }
                } else {
                    useVertex[i] = true;
                    ++usedVertices;
                    if(currentNode.right == null){
                        currentNode = generateNewChainAndReturnLeafNode(false, currentNode, cacheableVertices,
                            useVertex, i, usedVertices, verticesSize, query, result, allLeafParents);
                        break;
                    } else {
                        currentNode = currentNode.right;
                    }
                }
            }
            ((DataTreeNode<KeysQueriesGroup<Object, SliceQuery>>) currentNode).data.getQueries().add(query);
        }
        return new MultiKeysQueryGroups<>(result, new MultiQueriesByKeysGroupsContext<>(vertexIds, root, queries.size(), allLeafParents));
    }

    private static InternalVertex[] filterNonCacheableVertices(Collection<InternalVertex> vertices, MutableInt verticesMutableSize){
        InternalVertex[] cacheableVertices = new InternalVertex[vertices.size()];
        int i = 0;
        for(InternalVertex v : vertices){
            if(!v.isNew() && v.hasId() && (v instanceof CacheVertex)){
                cacheableVertices[i++] = v;
            }
        }
        verticesMutableSize.setValue(i);
        return cacheableVertices;
    }

    private static Object[] toPartiallyFilledVertexIds(InternalVertex[] cacheableVertices, boolean[] useVertex, int fillUpToIndex, int totalVerticesSize){
        Object[] vertexIds = new Object[totalVerticesSize];
        for(int i=0, j=0; i<=fillUpToIndex; i++){
            if(useVertex[i]){
                vertexIds[j++] = cacheableVertices[i].id();
            }
        }
        return vertexIds;
    }

    private static Object[] toIds(InternalVertex[] cacheableVertices, int totalVerticesSize){
        Object[] vertexIds = new Object[totalVerticesSize];
        for(int i=0; i<totalVerticesSize; i++){
            vertexIds[i] = cacheableVertices[i].id();
        }
        return vertexIds;
    }

    private static TreeNode generateNewChainAndReturnLeafNode(boolean childNodeHasLoadedRelations, TreeNode currentNode, InternalVertex[] cacheableVertices,
                                                              boolean[] useVertex, int currentIndex, int usedVertices, int totalVerticesSize,
                                                              SliceQuery query, List<KeysQueriesGroup<Object, SliceQuery>> result,
                                                              List<TreeNode> allLeafParents){
        TreeNode previousNode = currentNode;
        currentNode = new TreeNode();
        assignChild(previousNode, currentNode, childNodeHasLoadedRelations);
        Object[] queryVertexIds = toPartiallyFilledVertexIds(cacheableVertices, useVertex, currentIndex, totalVerticesSize);
        ChainCreationResult newChain = generateNewNodesChain(
            cacheableVertices, queryVertexIds, previousNode, currentNode, currentIndex, usedVertices, totalVerticesSize, childNodeHasLoadedRelations, query);
        previousNode = newChain.latestParent;
        usedVertices = newChain.usedVerticesCount;
        if(usedVertices != totalVerticesSize){
            if(usedVertices == 0){
                queryVertexIds = new Object[0];
            } else {
                Object[] trimmedQueryVertexIds = new Object[usedVertices];
                System.arraycopy(queryVertexIds, 0, trimmedQueryVertexIds, 0, usedVertices);
                queryVertexIds = trimmedQueryVertexIds;
            }
        }

        KeysQueriesGroup<Object, SliceQuery> data = new KeysQueriesGroup<>(Arrays.asList(queryVertexIds), new ArrayList<>());
        currentNode = new DataTreeNode<>(data);
        assignChild(previousNode, currentNode, newChain.lastVertexHasLoadedRelations);
        if(previousNode.left == null || previousNode.right == null){
            allLeafParents.add(previousNode);
        }
        if(usedVertices > 0){
            result.add(data);
        }
        return currentNode;
    }

    private static void assignChild(TreeNode parent, TreeNode child, boolean childNodeHasLoadedRelations){
        if(childNodeHasLoadedRelations){
            parent.left = child;
        } else {
            parent.right = child;
        }
    }

    private static ChainCreationResult generateNewNodesChain(InternalVertex[] cacheableVertices,
                                                             Object[] queryVertexIds,
                                                             TreeNode previousNode,
                                                             TreeNode currentNode,
                                                             int currentIndex,
                                                             int usedVertices,
                                                             int totalVerticesSize,
                                                             boolean lastChildHasLoadedRelations,
                                                             SliceQuery currentQuery){
        boolean hasLoadedRelations = lastChildHasLoadedRelations;
        while (++currentIndex < totalVerticesSize){
            previousNode = currentNode;
            hasLoadedRelations = cacheableVertices[currentIndex].hasLoadedRelations(currentQuery);
            if(hasLoadedRelations){
                currentNode.left = new TreeNode();
                currentNode = currentNode.left;
            } else {
                currentNode.right = new TreeNode();
                currentNode = currentNode.right;
                queryVertexIds[usedVertices++] = cacheableVertices[currentIndex].id();
            }
        }
        return new ChainCreationResult(previousNode, hasLoadedRelations, usedVertices);
    }

    public static class TreeNode {
        public TreeNode left;
        public TreeNode right;
        public TreeNode() {
        }
        public TreeNode(TreeNode left, TreeNode right) {
            this.left = left;
            this.right = right;
        }
    }

    public static class DataTreeNode<Q> extends TreeNode{
        public final Q data;
        public DataTreeNode(Q data) {
            this.data = data;
        }
    }

    private static class ChainCreationResult{
        // parent before the leaf node
        public final TreeNode latestParent;
        public final boolean lastVertexHasLoadedRelations;
        public final int usedVerticesCount;

        private ChainCreationResult(TreeNode latestParent, boolean lastVertexHasLoadedRelations, int usedVerticesCount) {
            this.latestParent = latestParent;
            this.lastVertexHasLoadedRelations = lastVertexHasLoadedRelations;
            this.usedVerticesCount = usedVerticesCount;
        }
    }
}
