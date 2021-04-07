// Copyright 2017 JanusGraph Authors
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

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal.Admin;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.LocalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.EdgeVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertiesStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.query.QueryUtil;
import org.janusgraph.graphdb.tinkerpop.optimize.JanusGraphTraversalUtil;
import org.janusgraph.graphdb.tinkerpop.optimize.step.HasStepFolder;
import org.janusgraph.graphdb.tinkerpop.optimize.step.JanusGraphEdgeVertexStep;
import org.janusgraph.graphdb.tinkerpop.optimize.step.JanusGraphPropertiesStep;
import org.janusgraph.graphdb.tinkerpop.optimize.step.JanusGraphVertexStep;
import org.janusgraph.graphdb.tinkerpop.optimize.step.MultiQueriable;

import java.util.Collections;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (https://markorodriguez.com)
 * @author Matthias Broecheler (http://matthiasb.com)
 */
public class JanusGraphLocalQueryOptimizerStrategy extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy> implements TraversalStrategy.ProviderOptimizationStrategy {

    private static final JanusGraphLocalQueryOptimizerStrategy INSTANCE = new JanusGraphLocalQueryOptimizerStrategy();

    private JanusGraphLocalQueryOptimizerStrategy() {
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        if (!traversal.getGraph().isPresent())
            return;

        final StandardJanusGraph janusGraph = JanusGraphTraversalUtil.getJanusGraph(traversal);
        if (janusGraph == null) {
            return;
        }

        boolean batchPropertyPrefetching = janusGraph.getConfiguration().batchPropertyPrefetching();
        int txVertexCacheSize = janusGraph.getConfiguration().getTxVertexCacheSize();

        applyJanusGraphVertexSteps(traversal, batchPropertyPrefetching, txVertexCacheSize);
        applyJanusGraphPropertiesSteps(traversal);
        inspectLocalTraversals(traversal);
    }

    private void applyJanusGraphVertexSteps(Admin<?, ?> traversal, boolean batchPropertyPrefetching, int txVertexCacheSize) {
        TraversalHelper.getStepsOfAssignableClass(VertexStep.class, traversal).forEach(originalStep -> {
            final JanusGraphVertexStep vertexStep = new JanusGraphVertexStep(originalStep);
            TraversalHelper.replaceStep(originalStep, vertexStep, originalStep.getTraversal());


            if (JanusGraphTraversalUtil.isEdgeReturnStep(vertexStep)) {
                HasStepFolder.foldInHasContainer(vertexStep, originalStep.getTraversal(), originalStep.getTraversal());
                //We cannot fold in orders or ranges since they are not local
            }

            assert JanusGraphTraversalUtil.isEdgeReturnStep(vertexStep) || JanusGraphTraversalUtil.isVertexReturnStep(vertexStep);
            final Step nextStep = JanusGraphTraversalUtil.getNextNonIdentityStep(vertexStep);
            if (nextStep instanceof RangeGlobalStep) {
                final int limit = QueryUtil.convertLimit(((RangeGlobalStep) nextStep).getHighRange());
                vertexStep.setLimit(0, QueryUtil.mergeHighLimits(limit, vertexStep.getHighLimit()));
            }

            if (batchPropertyPrefetching) {
                applyBatchPropertyPrefetching(originalStep.getTraversal(), vertexStep, nextStep, txVertexCacheSize);
            }
        });
    }

    private void applyJanusGraphPropertiesSteps(Admin<?, ?> traversal) {
        TraversalHelper.getStepsOfAssignableClass(PropertiesStep.class, traversal).forEach(originalStep -> {
            final JanusGraphPropertiesStep propertiesStep = new JanusGraphPropertiesStep(originalStep);
            TraversalHelper.replaceStep(originalStep, propertiesStep, originalStep.getTraversal());

            if (propertiesStep.getReturnType().forProperties()) {
                HasStepFolder.foldInHasContainer(propertiesStep, originalStep.getTraversal(), originalStep.getTraversal());
                //We cannot fold in orders or ranges since they are not local
            }
        });
    }

    private void inspectLocalTraversals(final Admin<?, ?> traversal) {
        TraversalHelper.getStepsOfClass(LocalStep.class, traversal).forEach(localStep -> {
            final Admin localTraversal = ((LocalStep<?, ?>) localStep).getLocalChildren().get(0);
            final Step localStart = localTraversal.getStartStep();

            if (localStart instanceof VertexStep) {
                final JanusGraphVertexStep vertexStep = new JanusGraphVertexStep((VertexStep) localStart);
                TraversalHelper.replaceStep(localStart, vertexStep, localTraversal);

                if (JanusGraphTraversalUtil.isEdgeReturnStep(vertexStep)) {
                    HasStepFolder.foldInHasContainer(vertexStep, localTraversal, traversal);
                    HasStepFolder.foldInOrder(vertexStep, vertexStep.getNextStep(), localTraversal, traversal, false, null);
                }
                HasStepFolder.foldInRange(vertexStep, JanusGraphTraversalUtil.getNextNonIdentityStep(vertexStep), localTraversal, null);


                unfoldLocalTraversal(traversal, localStep, localTraversal, vertexStep);
            }

            if (localStart instanceof PropertiesStep) {
                final JanusGraphPropertiesStep propertiesStep = new JanusGraphPropertiesStep((PropertiesStep) localStart);
                TraversalHelper.replaceStep(localStart, propertiesStep, localTraversal);

                if (propertiesStep.getReturnType().forProperties()) {
                    HasStepFolder.foldInHasContainer(propertiesStep, localTraversal, traversal);
                    HasStepFolder.foldInOrder(propertiesStep, propertiesStep.getNextStep(), localTraversal, traversal, false, null);
                }
                HasStepFolder.foldInRange(propertiesStep, JanusGraphTraversalUtil.getNextNonIdentityStep(propertiesStep), localTraversal, null);


                unfoldLocalTraversal(traversal, localStep, localTraversal, propertiesStep);
            }

        });
    }

    /**
     * If this step is followed by a subsequent has step then the properties will need to be
     * known when that has step is executed. The batch property pre-fetching optimisation
     * loads those properties into the vertex cache with a multiQuery preventing the need to
     * go back to the storage back-end for each vertex to fetch the properties.
     *
     * @param traversal         The traversal containing the step
     * @param vertexStep        The step to potentially apply the optimisation to
     * @param nextStep          The next step in the traversal
     * @param txVertexCacheSize The size of the vertex cache
     */
    private void applyBatchPropertyPrefetching(final Admin<?, ?> traversal, final JanusGraphVertexStep vertexStep, final Step nextStep, final int txVertexCacheSize) {
        if (Vertex.class.isAssignableFrom(vertexStep.getReturnClass())) {
            if (HasStepFolder.foldableHasContainerNoLimit(vertexStep)) {
                vertexStep.setBatchPropertyPrefetching(true);
                vertexStep.setTxVertexCacheSize(txVertexCacheSize);
            }
        }
        else if (nextStep instanceof EdgeVertexStep) {
            EdgeVertexStep edgeVertexStep = (EdgeVertexStep)nextStep;
            if (HasStepFolder.foldableHasContainerNoLimit(edgeVertexStep)) {
                JanusGraphEdgeVertexStep estep = new JanusGraphEdgeVertexStep(edgeVertexStep, txVertexCacheSize);
                TraversalHelper.replaceStep(nextStep, estep, traversal);
            }
        }
    }

    private static void unfoldLocalTraversal(final Traversal.Admin<?, ?> traversal,
                                             LocalStep<?, ?> localStep, Traversal.Admin localTraversal,
                                             MultiQueriable vertexStep) {
        assert localTraversal.asAdmin().getSteps().size() > 0;
        if (localTraversal.asAdmin().getSteps().size() == 1) {
            //Can replace the entire localStep by the vertex step in the outer traversal
            assert localTraversal.getStartStep() == vertexStep;
            vertexStep.setTraversal(traversal);
            TraversalHelper.replaceStep(localStep, vertexStep, traversal);
        }
    }

    private static final Set<Class<? extends ProviderOptimizationStrategy>> PRIORS = Collections.singleton(AdjacentVertexFilterOptimizerStrategy.class);


    @Override
    public Set<Class<? extends ProviderOptimizationStrategy>> applyPrior() {
        return PRIORS;
    }

    public static JanusGraphLocalQueryOptimizerStrategy instance() {
        return INSTANCE;
    }
}
