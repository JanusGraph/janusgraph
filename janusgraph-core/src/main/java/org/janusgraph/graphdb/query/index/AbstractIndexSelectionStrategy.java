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
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.JanusGraphElement;
import org.janusgraph.core.JanusGraphRelation;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.RelationType;
import org.janusgraph.core.attribute.Cmp;
import org.janusgraph.core.schema.JanusGraphSchemaType;
import org.janusgraph.core.schema.SchemaStatus;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.graphdb.database.IndexSerializer;
import org.janusgraph.graphdb.internal.ElementCategory;
import org.janusgraph.graphdb.internal.OrderList;
import org.janusgraph.graphdb.query.QueryUtil;
import org.janusgraph.graphdb.query.condition.And;
import org.janusgraph.graphdb.query.condition.Condition;
import org.janusgraph.graphdb.query.condition.MultiCondition;
import org.janusgraph.graphdb.query.condition.Or;
import org.janusgraph.graphdb.query.condition.PredicateCondition;
import org.janusgraph.graphdb.query.graph.JointIndexQuery;
import org.janusgraph.graphdb.types.CompositeIndexType;
import org.janusgraph.graphdb.types.IndexField;
import org.janusgraph.graphdb.types.IndexType;
import org.janusgraph.graphdb.types.MixedIndexType;
import org.janusgraph.graphdb.types.ParameterIndexField;
import org.janusgraph.graphdb.types.system.ImplicitKey;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.StreamSupport;
import javax.annotation.Nullable;

public abstract class AbstractIndexSelectionStrategy implements IndexSelectionStrategy {
    private static final double EQUAL_CONDITION_SCORE = 4;
    private static final double OTHER_CONDITION_SCORE = 1;
    private static final double CARDINALITY_SINGE_SCORE = 1000;
    private static final double CARDINALITY_OTHER_SCORE = 1000;

    public AbstractIndexSelectionStrategy(Configuration config) {

    }

    @Override
    public SelectedIndexQuery selectIndices(final ElementCategory resultType,
                                            final MultiCondition<JanusGraphElement> conditions,
                                            final Set<Condition> coveredClauses, OrderList orders,
                                            IndexSerializer serializer) {
        final Set<IndexType> rawCandidates = createIndexRawCandidates(conditions, resultType, serializer);
        return selectIndices(rawCandidates, conditions, coveredClauses, orders, serializer);
    }

    //Compile all indexes that cover at least one of the query conditions
    protected Set<IndexType> createIndexRawCandidates(final MultiCondition<JanusGraphElement> conditions,
                                                      final ElementCategory resultType, final IndexSerializer serializer) {
        return IndexSelectionUtil.getMatchingIndexes(conditions,
            indexType -> indexType.getElement() == resultType
                && !(conditions instanceof Or && (indexType.isCompositeIndex() || !serializer.features((MixedIndexType) indexType).supportNotQueryNormalForm()))
        );
    }

    /**
     * Creates an <code>IndexCandidate</code> from a <code>MultiCondition</code> which it covers.
     * @param index
     * @param conditions For the condition to be valid, it needs to match these rules:
     *                   <ul>
     *                   <li>It must be an equality condition</li>
     *                   <li>It must not cover multiple labels</li>
     *                   <li>The label must match the given <code>index</code></li>
     *                   </ul>
     * @return An instance of <code>IndexCandidate</code> if the parameter <code>conditions</code> is valid, <code>null</code> else.
     */
    @Nullable
    protected IndexCandidate createIndexCandidate(final IndexType index, final MultiCondition<JanusGraphElement> conditions, IndexSerializer serializer) {
        final Set<Condition> subCover = new HashSet<>(1);

        // Check that this index actually applies in case of a schema constraint
        if (index.hasSchemaTypeConstraint()) {
            final JanusGraphSchemaType type = index.getSchemaTypeConstraint();
            final Map.Entry<Condition, Collection<Object>> equalCon = getEqualityConditionValues(conditions, ImplicitKey.LABEL);

            if (equalCon == null) {
                // Only equality conditions are supported
                return null;
            }

            final Collection<Object> labels = equalCon.getValue();
            assert labels.size() >= 1;

            if (labels.size() > 1) {
                // The query optimizer currently does not support multiple label constraints
                return null;
            }
            if (!type.name().equals(labels.iterator().next())) {
                // Given IndexType does not match given condition label
                return null;
            }
            subCover.add(equalCon.getKey());
        }

        Object subCondition;
        if (index.isCompositeIndex()) {
            subCondition = indexCover((CompositeIndexType) index, conditions, subCover);
        } else {
            subCondition = indexCover((MixedIndexType) index, conditions, serializer, subCover);
        }
        if (subCondition == null || subCover.isEmpty()) {
            // Unable to initialize IndexCandidate from given parameters
            return null;
        }

        return new IndexCandidate(index, subCover, subCondition);
    }

    protected void addToJointQuery(final IndexCandidate indexCandidate, final JointIndexQuery jointQuery, final IndexSerializer serializer, final OrderList orders) {
        if (indexCandidate.getIndex().isCompositeIndex()) {
            jointQuery.add((CompositeIndexType) indexCandidate.getIndex(), serializer.getQuery(
                (CompositeIndexType) indexCandidate.getIndex(), (List<Object[]>) indexCandidate.getSubCondition()));
        } else {
            jointQuery.add((MixedIndexType) indexCandidate.getIndex(), serializer.getQuery(
                (MixedIndexType) indexCandidate.getIndex(), (Condition) indexCandidate.getSubCondition(), orders));
        }
    }

    protected double getConditionBasicScore(final Condition c) {
        if (c instanceof PredicateCondition && ((PredicateCondition) c).getPredicate() == Cmp.EQUAL) {
            return EQUAL_CONDITION_SCORE;
        } else {
            return OTHER_CONDITION_SCORE;
        }
    }

    protected double getIndexTypeScore(final IndexType index) {
        double score = 0.0;
        if (index.isCompositeIndex()) {
            if (((CompositeIndexType)index).getCardinality() == Cardinality.SINGLE) {
                score = CARDINALITY_SINGE_SCORE;
            } else {
                score = CARDINALITY_OTHER_SCORE;
            }
        }
        return score;
    }

    private List<Object[]> indexCover(final CompositeIndexType index, Condition<JanusGraphElement> condition,
                                     Set<Condition> covered) {
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
            return indexCovers;
        } else return null;
    }

    private void constructIndexCover(Object[] indexValues, int position, IndexField[] fields,
                                    Condition<JanusGraphElement> condition,
                                    List<Object[]> indexCovers, Set<Condition> coveredClauses) {
        if (position>=fields.length) {
            indexCovers.add(indexValues);
        } else {
            final IndexField field = fields[position];
            final Map.Entry<Condition,Collection<Object>> equalCon = getEqualityConditionValues(condition,field.getFieldKey());
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

    private Condition<JanusGraphElement> indexCover(final MixedIndexType index,
                                                   Condition<JanusGraphElement> condition,
                                                   final IndexSerializer indexInfo,
                                                   final Set<Condition> covered) {
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
            return condition;
        }
        assert condition instanceof And;
        final And<JanusGraphElement> subCondition = new And<>(condition.numChildren());
        for (final Condition<JanusGraphElement> subClause : condition.getChildren()) {
            if (coversAll(index,subClause,indexInfo)) {
                subCondition.add(subClause);
                covered.add(subClause);
            }
        }
        return subCondition.isEmpty()?null:subCondition;
    }

    private boolean coversAll(final MixedIndexType index, Condition<JanusGraphElement> condition,
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

    private Map.Entry<Condition,Collection<Object>> getEqualityConditionValues(
        Condition<JanusGraphElement> condition, RelationType type) {
        for (final Condition c : condition.getChildren()) {
            if (c instanceof Or) {
                final Map.Entry<RelationType,Collection> orEqual = QueryUtil.extractOrCondition((Or)c);
                if (orEqual!=null && orEqual.getKey().equals(type) && !orEqual.getValue().isEmpty()) {
                    return new AbstractMap.SimpleImmutableEntry(c,orEqual.getValue());
                }
            } else if (c instanceof PredicateCondition) {
                final PredicateCondition<RelationType, JanusGraphRelation> atom = (PredicateCondition)c;
                if (atom.getKey().equals(type) && atom.getPredicate()== Cmp.EQUAL && atom.getValue()!=null) {
                    return new AbstractMap.SimpleImmutableEntry(c,Collections.singletonList(atom.getValue()));
                }
            }

        }
        return null;
    }
}
