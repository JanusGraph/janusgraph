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

package org.janusgraph.graphdb.tinkerpop.optimize.strategy;

import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.FilterStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.NoneStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.IdStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.JanusGraphElement;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.query.JanusGraphPredicateUtils;
import org.janusgraph.graphdb.query.QueryUtil;
import org.janusgraph.graphdb.query.condition.MultiCondition;
import org.janusgraph.graphdb.query.condition.PredicateCondition;
import org.janusgraph.graphdb.query.index.IndexSelectionUtil;
import org.janusgraph.graphdb.tinkerpop.optimize.JanusGraphTraversalUtil;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.janusgraph.graphdb.types.CompositeIndexType;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.janusgraph.graphdb.types.system.ImplicitKey.ADJACENT_ID;

/**
 * @author Florian Grieskamp (Florian.Grieskamp@gdata.de)
 */
public class AdjacentVertexHasUniquePropertyOptimizerStrategy
    extends AdjacentVertexOptimizerStrategy<HasStep<?>> {

    private static final AdjacentVertexHasUniquePropertyOptimizerStrategy INSTANCE =
        new AdjacentVertexHasUniquePropertyOptimizerStrategy();

    private AdjacentVertexHasUniquePropertyOptimizerStrategy() {}

    public static AdjacentVertexHasUniquePropertyOptimizerStrategy instance() {
        return INSTANCE;
    }

    @Override
    public Set<Class<? extends ProviderOptimizationStrategy>> applyPost() {
        Set<Class<? extends ProviderOptimizationStrategy>> postStrategies = new HashSet<>();
        postStrategies.add(JanusGraphStepStrategy.class);
        postStrategies.add(JanusGraphLocalQueryOptimizerStrategy.class);
        return postStrategies;
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        if (TraversalHelper.onGraphComputer(traversal) || !traversal.getGraph().isPresent()) {
            return;
        }

        final StandardJanusGraph janusGraph = JanusGraphTraversalUtil.getJanusGraph(traversal);
        if (janusGraph == null) {
            return;
        }

        if (!janusGraph.getConfiguration().optimizerBackendAccess()) {
            return;
        }

        TraversalHelper.getStepsOfClass(HasStep.class, traversal)
            .forEach(this::optimizeStep);
    }

    /**
     * Determines whether this HasStep can be answered by a unique index and thus, returns either 0 or 1 match
     */
    @Override
    protected boolean isValidStep(HasStep<?> step) {
        StandardJanusGraphTx tx = (StandardJanusGraphTx) JanusGraphTraversalUtil.getTx(step.getTraversal());

        List<String> givenKeys = step.getHasContainers().stream()
            .map(HasContainer::getKey).collect(Collectors.toList());

        List<PredicateCondition<String, JanusGraphElement>> constraints = step.getHasContainers().stream()
            .filter(hc -> hc.getBiPredicate() == Compare.eq)
            .map(hc -> new PredicateCondition<>(hc.getKey(), JanusGraphPredicateUtils.convert(hc.getBiPredicate()), hc.getValue()))
            .filter(pc -> tx.validDataType(pc.getValue().getClass()))
            .collect(Collectors.toList());
        final MultiCondition<JanusGraphElement> conditions = QueryUtil.constraints2QNF(tx, constraints);

        // check all matching unique indexes
        return IndexSelectionUtil.existsMatchingIndex(conditions,
            indexType -> indexType.isCompositeIndex()
                && ((CompositeIndexType) indexType).getCardinality() == Cardinality.SINGLE
                && IndexSelectionUtil.isIndexSatisfiedByGivenKeys(indexType, givenKeys));
    }

    private Traversal.Admin<?,Long> generateFilter(Traversal.Admin<?,?> traversal, HasStep<?> originalStep) {
        Traversal.Admin filterTraversal = new DefaultGraphTraversal<>(traversal.getGraph().get())
            .addStep(
                new GraphStep<>(traversal, Vertex.class, true)
            );

        filterTraversal.addStep(originalStep);
        filterTraversal.addStep(new IdStep<>(filterTraversal));
        return filterTraversal;
    }

    @Override
    protected FilterStep<Edge> makeFilterByAdjacentIdStep(Traversal.Admin<?, ?> traversal, HasStep<?> originalStep) {
        Traversal.Admin<?,Long> filterTraversal = generateFilter(traversal, originalStep);
        if (filterTraversal.hasNext()) {
            HasContainer hc = new HasContainer(ADJACENT_ID.name(), P.eq(filterTraversal.next()));
            return new HasStep<>(traversal, hc);
        } else {
            return new NoneStep<>(traversal);
        }
    }
}
