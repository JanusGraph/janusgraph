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
import org.janusgraph.graphdb.query.index.candidate.AbstractIndexCandidate;
import org.janusgraph.graphdb.query.index.candidate.IndexCandidateFactory;
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
                                            OrderList orders,
                                            IndexSerializer serializer) {
        final Set<IndexType> rawCandidates = createIndexRawCandidates(conditions, resultType, serializer);
        return selectIndices(rawCandidates, conditions, orders, serializer);
    }

    //Compile all indexes that cover at least one of the query conditions
    protected Set<IndexType> createIndexRawCandidates(final MultiCondition<JanusGraphElement> conditions,
                                                      final ElementCategory resultType, final IndexSerializer serializer) {
        return IndexSelectionUtil.getMatchingIndexes(conditions,
            indexType -> indexType.getElement() == resultType
                && !(conditions instanceof Or && (indexType.isCompositeIndex() || !serializer.features((MixedIndexType) indexType).supportNotQueryNormalForm()))
        );
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

}
