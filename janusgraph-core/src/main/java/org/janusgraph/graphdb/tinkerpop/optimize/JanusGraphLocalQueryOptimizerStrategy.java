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

package org.janusgraph.graphdb.tinkerpop.optimize;

import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.BranchStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.OptionalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MatchStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.PathUtil;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.query.QueryUtil;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.LocalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertiesStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.javatuples.Pair;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (https://markorodriguez.com)
 * @author Matthias Broecheler (http://matthiasb.com)
 */
public class JanusGraphLocalQueryOptimizerStrategy extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy> implements TraversalStrategy.ProviderOptimizationStrategy {

    private static final JanusGraphLocalQueryOptimizerStrategy INSTANCE = new JanusGraphLocalQueryOptimizerStrategy();

    private static final List<Class<? extends Step>> MULTIQUERY_INCOMPATIBLE_STEPS =
        Arrays.asList(RepeatStep.class, MatchStep.class, BranchStep.class, OptionalStep.class);

    private JanusGraphLocalQueryOptimizerStrategy() {
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        if (!traversal.getGraph().isPresent())
            return;

        final Graph graph = traversal.getGraph().get();

        //If this is a compute graph then we can't apply local traversal optimisation at this stage.
        final StandardJanusGraph janusGraph = graph instanceof StandardJanusGraphTx ? ((StandardJanusGraphTx) graph).getGraph() : (StandardJanusGraph) graph;
        final boolean useMultiQuery = !TraversalHelper.onGraphComputer(traversal) && janusGraph.getConfiguration().useMultiQuery();

        /*
                ====== VERTEX STEP ======
         */

        TraversalHelper.getStepsOfClass(VertexStep.class, traversal).forEach(originalStep -> {
            final JanusGraphVertexStep vertexStep = new JanusGraphVertexStep(originalStep);
            TraversalHelper.replaceStep(originalStep, vertexStep, traversal);


            if (JanusGraphTraversalUtil.isEdgeReturnStep(vertexStep)) {
                HasStepFolder.foldInHasContainer(vertexStep, traversal, traversal);
                //We cannot fold in orders or ranges since they are not local
            }

            assert JanusGraphTraversalUtil.isEdgeReturnStep(vertexStep) || JanusGraphTraversalUtil.isVertexReturnStep(vertexStep);
            final Step nextStep = JanusGraphTraversalUtil.getNextNonIdentityStep(vertexStep);
            if (nextStep instanceof RangeGlobalStep) {
                final int limit = QueryUtil.convertLimit(((RangeGlobalStep) nextStep).getHighRange());
                vertexStep.setLimit(0, QueryUtil.mergeHighLimits(limit, vertexStep.getHighLimit()));
            }

            if (useMultiQuery && !(isChildOf(vertexStep, MULTIQUERY_INCOMPATIBLE_STEPS))) {
                vertexStep.setUseMultiQuery(true);
            }
        });


        /*
                ====== PROPERTIES STEP ======
         */


        TraversalHelper.getStepsOfClass(PropertiesStep.class, traversal).forEach(originalStep -> {
            final JanusGraphPropertiesStep propertiesStep = new JanusGraphPropertiesStep(originalStep);
            TraversalHelper.replaceStep(originalStep, propertiesStep, traversal);


            if (propertiesStep.getReturnType().forProperties()) {
                HasStepFolder.foldInHasContainer(propertiesStep, traversal, traversal);
                //We cannot fold in orders or ranges since they are not local
            }

            if (useMultiQuery && !(isChildOf(propertiesStep, MULTIQUERY_INCOMPATIBLE_STEPS))) {
                propertiesStep.setUseMultiQuery(true);
            }
        });

        /*
                ====== EITHER INSIDE LOCAL ======
         */

        TraversalHelper.getStepsOfClass(LocalStep.class, traversal).forEach(localStep -> {
            final Traversal.Admin localTraversal = ((LocalStep<?, ?>) localStep).getLocalChildren().get(0);
            final Step localStart = localTraversal.getStartStep();

            if (localStart instanceof VertexStep) {
                final JanusGraphVertexStep vertexStep = new JanusGraphVertexStep((VertexStep) localStart);
                TraversalHelper.replaceStep(localStart, vertexStep, localTraversal);

                if (JanusGraphTraversalUtil.isEdgeReturnStep(vertexStep)) {
                    HasStepFolder.foldInHasContainer(vertexStep, localTraversal, traversal);
                    HasStepFolder.foldInOrder(vertexStep, vertexStep.getNextStep(), localTraversal, traversal, false, null);
                }
                HasStepFolder.foldInRange(vertexStep, JanusGraphTraversalUtil.getNextNonIdentityStep(vertexStep), localTraversal, null);


                unfoldLocalTraversal(traversal,localStep,localTraversal,vertexStep,useMultiQuery);
            }

            if (localStart instanceof PropertiesStep) {
                final JanusGraphPropertiesStep propertiesStep = new JanusGraphPropertiesStep((PropertiesStep) localStart);
                TraversalHelper.replaceStep(localStart, propertiesStep, localTraversal);

                if (propertiesStep.getReturnType().forProperties()) {
                    HasStepFolder.foldInHasContainer(propertiesStep, localTraversal, traversal);
                    HasStepFolder.foldInOrder(propertiesStep, propertiesStep.getNextStep(), localTraversal, traversal, false, null);
                }
                HasStepFolder.foldInRange(propertiesStep, JanusGraphTraversalUtil.getNextNonIdentityStep(propertiesStep), localTraversal, null);


                unfoldLocalTraversal(traversal,localStep,localTraversal,propertiesStep,useMultiQuery);
            }

        });
    }

    private static void unfoldLocalTraversal(final Traversal.Admin<?, ?> traversal,
                                             LocalStep<?,?> localStep, Traversal.Admin localTraversal,
                                             MultiQueriable vertexStep, boolean useMultiQuery) {
        assert localTraversal.asAdmin().getSteps().size() > 0;
        if (localTraversal.asAdmin().getSteps().size() == 1) {
            //Can replace the entire localStep by the vertex step in the outer traversal
            assert localTraversal.getStartStep() == vertexStep;
            vertexStep.setTraversal(traversal);
            TraversalHelper.replaceStep(localStep, vertexStep, traversal);

            if (useMultiQuery && !(isChildOf(vertexStep, MULTIQUERY_INCOMPATIBLE_STEPS))) {
                vertexStep.setUseMultiQuery(true);
            }
        }
    }

    private static boolean isChildOf(Step<?, ?> currentStep, List<Class<? extends Step>> stepClasses) {
        Step<?, ?> parent = currentStep.getTraversal().getParent().asStep();
        while (!parent.equals(EmptyStep.instance())) {
            final Step<?, ?> p = parent;
            if(stepClasses.stream().filter(stepClass -> stepClass.isInstance(p)).findFirst().isPresent()) {
                return true;
            }
            parent = parent.getTraversal().getParent().asStep();
        }
        return false;
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
