// Copyright 2022 JanusGraph Authors
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

package org.janusgraph.graphdb.query.index.candidate;

import org.janusgraph.core.JanusGraphElement;
import org.janusgraph.core.schema.JanusGraphSchemaType;
import org.janusgraph.core.schema.SchemaStatus;
import org.janusgraph.graphdb.database.IndexSerializer;
import org.janusgraph.graphdb.internal.OrderList;
import org.janusgraph.graphdb.query.QueryUtil;
import org.janusgraph.graphdb.query.condition.And;
import org.janusgraph.graphdb.query.condition.Condition;
import org.janusgraph.graphdb.query.condition.MultiCondition;
import org.janusgraph.graphdb.query.condition.Or;
import org.janusgraph.graphdb.types.CompositeIndexType;
import org.janusgraph.graphdb.types.IndexField;
import org.janusgraph.graphdb.types.IndexType;
import org.janusgraph.graphdb.types.MixedIndexType;
import org.janusgraph.graphdb.types.system.ImplicitKey;
import org.janusgraph.util.datastructures.Combinatorics;
import org.janusgraph.util.datastructures.Pair;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

public class IndexCandidateFactory {

    @Nullable
    public static <E extends JanusGraphElement> AbstractIndexCandidate<? extends IndexType, E> build(final IndexType index,
                                                                                                     final MultiCondition<E> conditions,
                                                                                                     final IndexSerializer serializer,
                                                                                                     OrderList orders) {
        final Set<Condition<E>> subCover = new HashSet<>(1);

        // Check that this index actually applies in case of a schema constraint
        if (index.hasSchemaTypeConstraint()) {
            final JanusGraphSchemaType type = index.getSchemaTypeConstraint();

            // Only equality conditions are supported
            final Pair<Condition<E>, Collection<Object>> labelCondition = QueryUtil.getEqualityConditionValues(conditions, ImplicitKey.LABEL);
            if (labelCondition == null) return null;

            final Collection<Object> labels = labelCondition.getValue();
            assert labels.size() >= 1;

            // The query optimizer currently does not support multiple label constraints
            if (labels.size() > 1) return null;

            // Given IndexType does not match given condition label
            if (!type.name().equals(labels.iterator().next())) return null;

            subCover.add(labelCondition.getKey());
        }

        if (index.isCompositeIndex()) {
            return indexCover((CompositeIndexType) index, conditions, subCover, orders);
        } else {
            return indexCover((MixedIndexType) index, conditions, serializer, subCover, orders);
        }
    }

    private static <E extends JanusGraphElement> AbstractIndexCandidate<CompositeIndexType, E> indexCover(final CompositeIndexType index,
                                                                                                          Condition<E> completeCondition,
                                                                                                          Set<Condition<E>> alreadyCovered,
                                                                                                          OrderList orders) {
        if (!QueryUtil.isQueryNormalForm(completeCondition)) return null;
        assert completeCondition instanceof And;
        if (index.getStatus()!= SchemaStatus.ENABLED) return null;

        final IndexField[] fields = index.getFieldKeys();
        final Set<Condition<E>> coveredClauses = new HashSet<>(fields.length);
        final ArrayList<List<Object>> indexValues = new ArrayList<>(fields.length);

        for (IndexField field : fields) {
            final Pair<Condition<E>,Collection<Object>> equalCon = QueryUtil.getEqualityConditionValues(completeCondition, field.getFieldKey());
            if (equalCon == null) return null;
            coveredClauses.add(equalCon.getKey());
            indexValues.add(new ArrayList<>(equalCon.getValue()));
        }

        List<Object[]> subCondition = Combinatorics.cartesianProduct(indexValues).stream()
            .map(List::toArray)
            .collect(Collectors.toList());

        if (subCondition.isEmpty()) return null;
        alreadyCovered.addAll(coveredClauses);
        return new CompositeIndexCandidate<>(index, alreadyCovered, subCondition, orders);
    }

    private static <E extends JanusGraphElement> AbstractIndexCandidate<MixedIndexType, E> indexCover(final MixedIndexType index,
                                                                     Condition<E> completeCondition,
                                                                     final IndexSerializer indexInfo,
                                                                     final Set<Condition<E>> alreadyCovered,
                                                                     OrderList orders) {
        if (!indexInfo.features(index).supportNotQueryNormalForm() && !QueryUtil.isQueryNormalForm(completeCondition)) return null;

        if (completeCondition instanceof Or) {
            if (index.coversAll(completeCondition, indexInfo)) {
                alreadyCovered.add(completeCondition);
                return new MixedIndexCandidate<>(index, alreadyCovered, completeCondition, orders);
            } else {
                return null;
            }
        }

        assert completeCondition instanceof And;
        final And<E> subCondition = new And<>(completeCondition.numChildren());
        for (final Condition<E> subClause : completeCondition.getChildren()) {
            if (index.coversAll(subClause, indexInfo)) {
                subCondition.add(subClause);
                alreadyCovered.add(subClause);
            }
        }

        return subCondition.isEmpty() ? null : new MixedIndexCandidate<>(index, alreadyCovered, subCondition, orders);
    }
}
