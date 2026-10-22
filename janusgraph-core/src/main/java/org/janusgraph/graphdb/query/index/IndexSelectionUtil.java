// Copyright 2020 JanusGraph Authors
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

package org.janusgraph.graphdb.query.index;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.JanusGraphElement;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.RelationType;
import org.janusgraph.graphdb.database.IndexSerializer;
import org.janusgraph.graphdb.internal.InternalRelationType;
import org.janusgraph.graphdb.internal.OrderList;
import org.janusgraph.graphdb.query.condition.Condition;
import org.janusgraph.graphdb.query.condition.ConditionUtil;
import org.janusgraph.graphdb.query.condition.MultiCondition;
import org.janusgraph.graphdb.query.condition.PredicateCondition;
import org.janusgraph.graphdb.types.IndexField;
import org.janusgraph.graphdb.types.IndexType;
import org.janusgraph.graphdb.types.MixedIndexType;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Predicate;

public class IndexSelectionUtil {

    /**
     * Whether the mixed index holds each key of the order. An index whose backend can't sort by a key, such as a key
     * of several values or a string key of TEXT mapping, isn't given an order by it, so where a strategy holds such an
     * index to sort the results by this method, JanusGraph sorts them itself.
     *
     * @deprecated use {@link #indexCoversOrder(MixedIndexType, OrderList, IndexSerializer)}, which also checks that
     * the index backend can sort by each key
     */
    @Deprecated
    public static boolean indexCoversOrder(MixedIndexType index, OrderList orders) {
        for (int i = 0; i < orders.size(); i++) {
            if (!index.indexesKey(orders.getKey(i)))
                return false;
        }
        return true;
    }

    /**
     * Whether the mixed index can sort the elements in the order: it holds each of the order keys, its backend can
     * sort by each of them ({@link IndexSerializer#supportsOrdering}), as none of JanusGraph's backends can sort by a
     * string key of TEXT mapping, and its backend can sort by a key of several values, {@link Cardinality#LIST} or
     * {@link Cardinality#SET}, where the order has one, as Elasticsearch, OpenSearch and Solr can and Lucene can't.
     * Where it can't, JanusGraph sorts the elements itself.
     */
    public static boolean indexCoversOrder(MixedIndexType index, OrderList orders, IndexSerializer serializer) {
        for (int i = 0; i < orders.size(); i++) {
            final PropertyKey key = orders.getKey(i);
            if (!index.indexesKey(key) || !serializer.supportsOrdering(index, index.getField(key))) {
                return false;
            }
            final Cardinality cardinality = key.cardinality();
            if ((cardinality == Cardinality.LIST || cardinality == Cardinality.SET)
                    && !serializer.features(index).supportsOrderingListProperty()) {
                return false;
            }
        }
        return true;
    }

    private static Iterable<IndexType> getKeyIndexesForCondition(Condition<JanusGraphElement> condition) {
        if (condition instanceof PredicateCondition) {
            final RelationType type = ((PredicateCondition<RelationType, JanusGraphElement>) condition).getKey();
            Preconditions.checkArgument(type != null && type.isPropertyKey());
            return ((InternalRelationType) type).getKeyIndexes();
        }
        return Collections.emptySet();
    }

    public static Set<IndexType> getMatchingIndexes(MultiCondition<JanusGraphElement> conditions) {
        return getMatchingIndexes(conditions, i -> true);
    }

    public static Set<IndexType> getMatchingIndexes(MultiCondition<JanusGraphElement> conditions, Predicate<IndexType> filter) {
        if (conditions == null || filter == null) {
            return Collections.emptySet();
        }
        final Set<IndexType> availableIndexes = new HashSet<>();
        ConditionUtil.traversal(conditions, condition -> {
            for (IndexType indexCandidate : getKeyIndexesForCondition(condition)) {
                if (filter.test(indexCandidate)) {
                    availableIndexes.add(indexCandidate);
                }
            }
            return true;
        });
        return availableIndexes;
    }

    public static boolean existsMatchingIndex(MultiCondition<JanusGraphElement> conditions)  {
        return existsMatchingIndex(conditions, i -> true);
    }

    public static boolean existsMatchingIndex(MultiCondition<JanusGraphElement> conditions, Predicate<IndexType> filter) {
        if (conditions == null || filter == null) {
            return false;
        }
        MutableBoolean exists = new MutableBoolean();
        ConditionUtil.traversal(conditions, condition -> {
            if(exists.isFalse()){
                for (IndexType indexCandidate : getKeyIndexesForCondition(condition)) {
                    if (filter.test(indexCandidate)) {
                        exists.setTrue();
                        return true;
                    }
                }
            }
            return true;
        });
        return exists.booleanValue();
    }

    public static boolean isIndexSatisfiedByGivenKeys(IndexType index, Collection<String> givenKeys) {
        for (IndexField indexField : index.getFieldKeys()) {
            if (!givenKeys.contains(indexField.getFieldKey().name())) {
                return false;
            }
        }
        return true;
    }
}
