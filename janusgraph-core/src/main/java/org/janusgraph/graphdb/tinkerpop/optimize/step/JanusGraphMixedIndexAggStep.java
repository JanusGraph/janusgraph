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

package org.janusgraph.graphdb.tinkerpop.optimize.step;

import com.google.common.base.Preconditions;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.Profiling;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ReducingBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.process.traversal.util.MutableMetrics;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.MixedIndexAggQuery;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.RelationType;
import org.janusgraph.core.schema.SchemaStatus;
import org.janusgraph.graphdb.internal.ElementCategory;
import org.janusgraph.graphdb.query.condition.ConditionUtil;
import org.janusgraph.graphdb.query.condition.PredicateCondition;
import org.janusgraph.graphdb.query.graph.GraphCentricQuery;
import org.janusgraph.graphdb.query.graph.JointIndexQuery;
import org.janusgraph.graphdb.query.graph.MixedIndexAggQueryBuilder;
import org.janusgraph.graphdb.query.profile.QueryProfiler;
import org.janusgraph.graphdb.tinkerpop.optimize.JanusGraphTraversalUtil;
import org.janusgraph.graphdb.tinkerpop.profile.TP3ProfileWrapper;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.janusgraph.graphdb.types.MixedIndexType;
import org.janusgraph.graphdb.types.system.ImplicitKey;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import static org.janusgraph.graphdb.database.util.IndexRecordUtil.key2Field;

/**
 * A custom step similar to {@link org.apache.tinkerpop.gremlin.process.traversal.step.map.CountGlobalStep} but
 * uses mixed index query to directly fetch number of satisfying elements without actually fetching the elements.
 *
 * @author Boxuan Li (liboxuan@connect.hku.hk)
 */
public class JanusGraphMixedIndexAggStep<S> extends ReducingBarrierStep<S, Number> implements Profiling {

    private final ArrayList<HasContainer> hasContainers = new ArrayList<>();
    private MixedIndexAggQuery mixedIndexAggQuery = null;
    private boolean done;
    private final Aggregation aggregation;
    // the offset of a range the traversal has folded into its query, which a count leaves out
    private final int offset;

    public JanusGraphMixedIndexAggStep(JanusGraphStep janusGraphStep, Traversal.Admin<?, ?> traversal, Aggregation agg) {
        super(traversal);
        JanusGraphTransaction tx = JanusGraphTraversalUtil.getTx(traversal);
        aggregation = agg;
        offset = janusGraphStep.getLowLimit();
        final MixedIndexAggQueryBuilder aggregationQueryBuilder = (MixedIndexAggQueryBuilder) tx.mixedIndexAggQuery();

        final GraphCentricQuery query = janusGraphStep.buildGlobalGraphCentricQuery();

        if (query != null && aggregatesWhatTheTraversalWould(query, tx)) {
            final String fieldName = agg.getFieldName();
            boolean isIndexed = false;
            if (fieldName == null) {
                isIndexed = true;
            } else {
                MixedIndexType indexType = (MixedIndexType)query.getIndexQuery().getBackendQuery().getQuery(0).getIndex();
                // a field which isn't enabled may lack the values of the elements indexed before it was added
                Optional<? extends Class<?>> dataType = Arrays.stream(indexType.getFieldKeys())
                    .filter(f -> f.getStatus() == SchemaStatus.ENABLED && f.getFieldKey().name().equals(fieldName))
                    .map(f -> f.getFieldKey().dataType())
                    .filter(Number.class::isAssignableFrom)
                    .findFirst();
                if (dataType.isPresent()) {
                    isIndexed = true;
                    aggregation.setDataType(dataType.get());
                    RelationType rt = tx.getRelationType(fieldName);
                    if (rt instanceof PropertyKey)
                        aggregation.setFieldName(key2Field(indexType, (PropertyKey) rt));
                }
            }
            if (isIndexed) {
                // The limit of the traversal, which a count stops at: the query builder sizes the limit of the index
                // query to fetch the elements in batches, larger for conditions the index doesn't cover and for the
                // changes of a transaction, and smaller under query.smart-limit or query.hard-max-limit
                final JointIndexQuery indexQuery = query.getIndexQuery().getBackendQuery().updateLimit(query.getLimit());
                mixedIndexAggQuery = aggregationQueryBuilder.constructIndex(indexQuery,
                    Vertex.class.isAssignableFrom(janusGraphStep.getReturnClass()) ? ElementCategory.VERTEX : ElementCategory.EDGE);

            }
        }
    }

    // Whether the index, which aggregates every match of the query among the elements it holds, gives the same answer
    // as the traversal
    private boolean aggregatesWhatTheTraversalWould(GraphCentricQuery query, JanusGraphTransaction tx) {
        // The aggregation runs on a single mixed index subquery only. A query whose keys don't exist has none, and one
        // whose conditions a composite index answers has no mixed one to aggregate on
        final JointIndexQuery indexQuery = query.getIndexQuery().getBackendQuery();
        final boolean singleMixedSubquery = query.getIndexQuery().isFitted() && indexQuery.size() == 1
            && indexQuery.getQuery(0).getIndex().isMixedIndex();
        // The index caps a count at the limit of the query, and the offset of a range is taken off a count, but the
        // other aggregations would cover the elements which a limit or an offset leave out
        final boolean sameMatches = aggregation.getType() == Aggregation.Type.COUNT || (offset == 0 && !query.hasLimit());
        return singleMixedSubquery && sameMatches && !changesAffect(query, tx);
    }

    // Whether the transaction has uncommitted changes, which the traversal sees and the index doesn't hold, that can
    // change the result: to a key of the query's condition, which can make an element match or stop matching it, or
    // to the aggregated key. A label in the condition can't change, and an element matches by the properties of the
    // other keys of a condition which a mixed index answers
    private boolean changesAffect(GraphCentricQuery query, JanusGraphTransaction tx) {
        if (!tx.hasModifications()) {
            return false;
        }
        if (!(tx instanceof StandardJanusGraphTx)) {
            return true;
        }
        final Set<PropertyKey> keys = new HashSet<>();
        final boolean[] otherCondition = {false};
        ConditionUtil.traversal(query.getCondition(), condition -> {
            if (condition instanceof PredicateCondition) {
                final Object key = ((PredicateCondition<?, ?>) condition).getKey();
                if (key instanceof ImplicitKey) {
                    otherCondition[0] |= key != ImplicitKey.LABEL;
                } else if (key instanceof PropertyKey) {
                    keys.add((PropertyKey) key);
                } else {
                    otherCondition[0] = true;
                }
            }
            return true;
        });
        if (aggregation.getFieldName() != null) {
            final RelationType aggregated = tx.getRelationType(aggregation.getFieldName());
            if (aggregated instanceof PropertyKey) {
                keys.add((PropertyKey) aggregated);
            } else {
                otherCondition[0] = true;
            }
        }
        return otherCondition[0] || keys.isEmpty()
            || ((StandardJanusGraphTx) tx).hasChangedProperties(query.getResultType(), keys);
    }

    @Override
    public Number projectTraverser(Traverser.Admin<S> traverser) {
        return traverser.bulk();
    }

    @Override
    public Traverser.Admin<Number> processNextStart() {
        if (!this.done) {
            this.done = true;
            // The strategy adds the step only once the query was built
            Preconditions.checkState(mixedIndexAggQuery != null, "No mixed index answers the aggregation of this step");
            // An index answers the minimum, maximum, mean or sum of no values with null: no result, as TinkerPop gives
            // when it aggregates nothing itself. A count is a number
            final Number result = this.mixedIndexAggQuery.execute(aggregation);
            if (result != null) {
                // a count leaves out the elements the traversal skips by the offset of a range
                final Number value = aggregation.getType() == Aggregation.Type.COUNT && offset > 0
                    ? Math.max(0L, result.longValue() - offset) : result;
                return getTraversal().getTraverserGenerator().generate(value, (Step) this, 1L);
            }
        }
        throw FastNoSuchElementException.instance();
    }

    @Override
    public String toString() {
        if (this.hasContainers.isEmpty()) {
            return super.toString();
        }
        return StringFactory.stepString(this, this.hasContainers);
    }

    @Override
    public void setMetrics(final MutableMetrics metrics) {
        QueryProfiler queryProfiler = new TP3ProfileWrapper(metrics);
        mixedIndexAggQuery.observeWith(queryProfiler);
    }

    public MixedIndexAggQuery getMixedIndexAggQuery() {
        return mixedIndexAggQuery;
    }
}
