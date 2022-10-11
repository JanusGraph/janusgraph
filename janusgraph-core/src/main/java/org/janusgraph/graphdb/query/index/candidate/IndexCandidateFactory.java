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

import com.google.common.base.Preconditions;
import org.janusgraph.core.JanusGraphElement;
import org.janusgraph.core.JanusGraphRelation;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.RelationType;
import org.janusgraph.core.attribute.Cmp;
import org.janusgraph.core.schema.JanusGraphSchemaType;
import org.janusgraph.core.schema.SchemaStatus;
import org.janusgraph.graphdb.database.IndexSerializer;
import org.janusgraph.graphdb.internal.OrderList;
import org.janusgraph.graphdb.query.QueryUtil;
import org.janusgraph.graphdb.query.condition.And;
import org.janusgraph.graphdb.query.condition.Condition;
import org.janusgraph.graphdb.query.condition.MultiCondition;
import org.janusgraph.graphdb.query.condition.Or;
import org.janusgraph.graphdb.query.condition.PredicateCondition;
import org.janusgraph.graphdb.types.CompositeIndexType;
import org.janusgraph.graphdb.types.IndexField;
import org.janusgraph.graphdb.types.IndexType;
import org.janusgraph.graphdb.types.MixedIndexType;
import org.janusgraph.graphdb.types.ParameterIndexField;
import org.janusgraph.graphdb.types.system.ImplicitKey;
import org.janusgraph.util.datastructures.Pair;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.StreamSupport;
import javax.annotation.Nullable;

public class IndexCandidateFactory {

    @Nullable
    public static AbstractIndexCandidate<? extends IndexType> build(final IndexType index,
                                                                    final MultiCondition conditions,
                                                                    final IndexSerializer serializer,
                                                                    OrderList orders) {
        final Set<Condition> subCover = new HashSet<>(1);

        // Check that this index actually applies in case of a schema constraint
        if (index.hasSchemaTypeConstraint()) {
            final JanusGraphSchemaType type = index.getSchemaTypeConstraint();

            // Only equality conditions are supported
            final Pair<Condition, Collection<Object>> labelCondition = getEqualityConditionValues(conditions, ImplicitKey.LABEL);
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


    private static AbstractIndexCandidate<CompositeIndexType> indexCover(final CompositeIndexType index, Condition<JanusGraphElement> condition,
                                      Set<Condition> covered, OrderList orders) {
        if (!QueryUtil.isQueryNormalForm(condition)) {
            return null;
        }
        assert condition instanceof And;
        if (index.getStatus()!= SchemaStatus.ENABLED) return null;
        final IndexField[] fields = index.getFieldKeys();
        final Object[] indexValues = new Object[fields.length];
        final Set<Condition> coveredClauses = new HashSet<>(fields.length);
        final List<Object[]> indexCovers = new ArrayList<>(4);

        constructIndexCover(indexValues,0,fields,condition,indexCovers,coveredClauses);
        if (!indexCovers.isEmpty()) {
            covered.addAll(coveredClauses);
            return new CompositeIndexCandidate(index, covered, indexCovers, orders);
        } else return null;
    }

    private static void constructIndexCover(Object[] indexValues, int position, IndexField[] fields,
                                     Condition<JanusGraphElement> condition,
                                     List<Object[]> indexCovers, Set<Condition> coveredClauses) {
        if (position>=fields.length) {
            indexCovers.add(indexValues);
        } else {
            final IndexField field = fields[position];
            final Pair<Condition, Collection<Object>> equalCon = getEqualityConditionValues(condition,field.getFieldKey());
            if (equalCon!=null) {
                coveredClauses.add(equalCon.getKey());
                assert equalCon.getValue().size()>0;
                for (final Object value : equalCon.getValue()) {
                    final Object[] newValues = Arrays.copyOf(indexValues,fields.length);
                    newValues[position]=value;
                    constructIndexCover(newValues,position+1,fields,condition,indexCovers,coveredClauses);
                }
            }
        }
    }

    private static AbstractIndexCandidate<MixedIndexType> indexCover(final MixedIndexType index,
                                                    Condition<JanusGraphElement> condition,
                                                    final IndexSerializer indexInfo,
                                                    final Set<Condition> covered,
                                                    final OrderList orders) {
        if (!indexInfo.features(index).supportNotQueryNormalForm() && !QueryUtil.isQueryNormalForm(condition)) {
            return null;
        }
        if (condition instanceof Or) {
            for (final Condition<JanusGraphElement> subClause : condition.getChildren()) {
                if (subClause instanceof And) {
                    for (final Condition<JanusGraphElement> subsubClause : subClause.getChildren()) {
                        if (!coversAll(index, subsubClause,indexInfo)) {
                            return null;
                        }
                    }
                } else {
                    if (!coversAll(index, subClause, indexInfo)) {
                        return null;
                    }
                }
            }
            covered.add(condition);
            return new MixedIndexCandidate(index, covered, condition, orders);
        }
        assert condition instanceof And;
        final And<JanusGraphElement> subCondition = new And<>(condition.numChildren());
        for (final Condition<JanusGraphElement> subClause : condition.getChildren()) {
            if (coversAll(index,subClause,indexInfo)) {
                subCondition.add(subClause);
                covered.add(subClause);
            }
        }
        return subCondition.isEmpty() ? null : new MixedIndexCandidate(index, covered, subCondition, orders);
    }


    private static boolean coversAll(final MixedIndexType index, Condition<JanusGraphElement> condition,
                              IndexSerializer indexInfo) {
        if (condition.getType()!=Condition.Type.LITERAL) {
            return StreamSupport.stream(condition.getChildren().spliterator(), false)
                .allMatch(child -> coversAll(index, child, indexInfo));
        }
        if (!(condition instanceof PredicateCondition)) {
            return false;
        }
        final PredicateCondition<RelationType, JanusGraphElement> atom = (PredicateCondition) condition;
        if (atom.getValue() == null && atom.getPredicate() != Cmp.NOT_EQUAL) {
            return false;
        }

        Preconditions.checkArgument(atom.getKey().isPropertyKey());
        final PropertyKey key = (PropertyKey) atom.getKey();
        final ParameterIndexField[] fields = index.getFieldKeys();
        final ParameterIndexField match = Arrays.stream(fields)
            .filter(field -> field.getStatus() == SchemaStatus.ENABLED)
            .filter(field -> field.getFieldKey().equals(key))
            .findAny().orElse(null);
        if (match == null) {
            return false;
        }
        boolean existsQuery = atom.getValue() == null && atom.getPredicate() == Cmp.NOT_EQUAL && indexInfo.supportsExistsQuery(index, match);
        return existsQuery || indexInfo.supports(index, match, atom.getPredicate());
    }

    private static Pair<Condition,Collection<Object>> getEqualityConditionValues(
        Condition<JanusGraphElement> condition, RelationType type) {
        for (final Condition c : condition.getChildren()) {
            if (c instanceof Or) {
                final Pair<RelationType,Collection<Object>> orEqual = QueryUtil.extractOrCondition((Or)c);
                if (orEqual!=null && orEqual.getKey().equals(type) && !orEqual.getValue().isEmpty()) {
                    return new Pair<>(c,orEqual.getValue());
                }
            } else if (c instanceof PredicateCondition) {
                final PredicateCondition<RelationType, JanusGraphRelation> atom = (PredicateCondition)c;
                if (atom.getKey().equals(type) && atom.getPredicate()== Cmp.EQUAL && atom.getValue()!=null) {
                    return new Pair<>(c, Collections.singletonList(atom.getValue()));
                }
            }

        }
        return null;
    }
}
