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

package org.janusgraph.diskstorage.keycolumnvalue;

import org.janusgraph.graphdb.util.MultiSliceQueriesGroupingUtil;

import java.util.List;

public class MultiQueriesByKeysGroupsContext<K> {

    /**
     * This is a list of all unique keys which could be used in this multi-query.
     * It doesn't mean that all these keys should be performed for each query.
     * This array should be considered to be a mata-data which may be useful for queries grouping and should not
     * be considered that all these keys should be requested.
     */
    private final K[] allKeysArr;

    /**
     * This is the tree where each left node represents a missing key from `allKeysArr` and each right node represents a selected
     * key from `allKeysArr`. The first child node represents the key with index 0 from `allKeysArr`,
     * while the leaf nodes are always represent a query with the index `allKeysArr.length-1`.
     * Moreover, each leaf node is of type {@code MultiSliceQueriesGroupingUtil.DataTreeNode<KeysQueriesGroup<K>>} ,
     * where `K` is of key type.
     * Each leaf node represents a separate group where all queries are unique.
     */
    private final MultiSliceQueriesGroupingUtil.TreeNode groupingRootTreeNode;

    /**
     * All parent nodes which has at least one child node which is a leaf node with data.
     */
    private final List<MultiSliceQueriesGroupingUtil.TreeNode> allLeafParents;

    private final int totalAmountOfQueries;

    public MultiQueriesByKeysGroupsContext(K[] allKeysArr, MultiQueriesByKeysGroupsContext<?> cloneContext) {
        this.allKeysArr = allKeysArr;
        this.groupingRootTreeNode = cloneContext.getGroupingRootTreeNode();
        this.totalAmountOfQueries = cloneContext.totalAmountOfQueries;
        this.allLeafParents = cloneContext.getAllLeafParents();
    }

    public MultiQueriesByKeysGroupsContext(K[] allKeysArr, MultiSliceQueriesGroupingUtil.TreeNode groupingRootTreeNode, int totalAmountOfQueries, List<MultiSliceQueriesGroupingUtil.TreeNode> allLeafParents) {
        this.allKeysArr = allKeysArr;
        this.groupingRootTreeNode = groupingRootTreeNode;
        this.totalAmountOfQueries = totalAmountOfQueries;
        this.allLeafParents = allLeafParents;
    }

    public K[] getAllKeysArr() {
        return allKeysArr;
    }

    public MultiSliceQueriesGroupingUtil.TreeNode getGroupingRootTreeNode() {
        return groupingRootTreeNode;
    }

    public int getTotalAmountOfQueries() {
        return totalAmountOfQueries;
    }

    public List<MultiSliceQueriesGroupingUtil.TreeNode> getAllLeafParents() {
        return allLeafParents;
    }
}
