package org.janusgraph.graphdb.tinkerpop.optimize;

import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.query.QueryUtil;
import org.janusgraph.graphdb.transaction.StandardJanusTx;
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

import java.util.Collections;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Matthias Broecheler (http://matthiasb.com)
 */
public class JanusLocalQueryOptimizerStrategy extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy> implements TraversalStrategy.ProviderOptimizationStrategy {

    private static final JanusLocalQueryOptimizerStrategy INSTANCE = new JanusLocalQueryOptimizerStrategy();

    private JanusLocalQueryOptimizerStrategy() {
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        if (!traversal.getGraph().isPresent())
            return;

        Graph graph = traversal.getGraph().get();

        //If this is a compute graph then we can't apply local traversal optimisation at this stage.
        StandardJanusGraph janusGraph = graph instanceof StandardJanusTx ? ((StandardJanusTx) graph).getGraph() : (StandardJanusGraph) graph;
        final boolean useMultiQuery = traversal.getEngine().isStandard() && janusGraph.getConfiguration().useMultiQuery();

        /*
                ====== VERTEX STEP ======
         */

        TraversalHelper.getStepsOfClass(VertexStep.class, traversal).forEach(originalStep -> {
            JanusVertexStep vstep = new JanusVertexStep(originalStep);
            TraversalHelper.replaceStep(originalStep, vstep, traversal);


            if (JanusTraversalUtil.isEdgeReturnStep(vstep)) {
                HasStepFolder.foldInHasContainer(vstep, traversal);
                //We cannot fold in orders or ranges since they are not local
            }

            assert JanusTraversalUtil.isEdgeReturnStep(vstep) || JanusTraversalUtil.isVertexReturnStep(vstep);
            Step nextStep = JanusTraversalUtil.getNextNonIdentityStep(vstep);
            if (nextStep instanceof RangeGlobalStep) {
                int limit = QueryUtil.convertLimit(((RangeGlobalStep) nextStep).getHighRange());
                vstep.setLimit(QueryUtil.mergeLimits(limit, vstep.getLimit()));
            }

            if (useMultiQuery) {
                vstep.setUseMultiQuery(true);
            }
        });


        /*
                ====== PROPERTIES STEP ======
         */


        TraversalHelper.getStepsOfClass(PropertiesStep.class, traversal).forEach(originalStep -> {
            JanusPropertiesStep vstep = new JanusPropertiesStep(originalStep);
            TraversalHelper.replaceStep(originalStep, vstep, traversal);


            if (vstep.getReturnType().forProperties()) {
                HasStepFolder.foldInHasContainer(vstep, traversal);
                //We cannot fold in orders or ranges since they are not local
            }

            if (useMultiQuery) {
                vstep.setUseMultiQuery(true);
            }
        });

        /*
                ====== EITHER INSIDE LOCAL ======
         */

        TraversalHelper.getStepsOfClass(LocalStep.class, traversal).forEach(localStep -> {
            Traversal.Admin localTraversal = ((LocalStep<?, ?>) localStep).getLocalChildren().get(0);
            Step localStart = localTraversal.getStartStep();

            if (localStart instanceof VertexStep) {
                JanusVertexStep vstep = new JanusVertexStep((VertexStep) localStart);
                TraversalHelper.replaceStep(localStart, vstep, localTraversal);

                if (JanusTraversalUtil.isEdgeReturnStep(vstep)) {
                    HasStepFolder.foldInHasContainer(vstep, localTraversal);
                    HasStepFolder.foldInOrder(vstep, localTraversal, traversal, false);
                }
                HasStepFolder.foldInRange(vstep, localTraversal);


                unfoldLocalTraversal(traversal,localStep,localTraversal,vstep,useMultiQuery);
            }

            if (localStart instanceof PropertiesStep) {
                JanusPropertiesStep vstep = new JanusPropertiesStep((PropertiesStep) localStart);
                TraversalHelper.replaceStep(localStart, vstep, localTraversal);

                if (vstep.getReturnType().forProperties()) {
                    HasStepFolder.foldInHasContainer(vstep, localTraversal);
                    HasStepFolder.foldInOrder(vstep, localTraversal, traversal, false);
                }
                HasStepFolder.foldInRange(vstep, localTraversal);


                unfoldLocalTraversal(traversal,localStep,localTraversal,vstep,useMultiQuery);
            }

        });
    }

    private static void unfoldLocalTraversal(final Traversal.Admin<?, ?> traversal,
                                             LocalStep<?,?> localStep, Traversal.Admin localTraversal,
                                             MultiQueriable vstep, boolean useMultiQuery) {
        assert localTraversal.asAdmin().getSteps().size() > 0;
        if (localTraversal.asAdmin().getSteps().size() == 1) {
            //Can replace the entire localStep by the vertex step in the outer traversal
            assert localTraversal.getStartStep() == vstep;
            vstep.setTraversal(traversal);
            TraversalHelper.replaceStep(localStep, vstep, traversal);

            if (useMultiQuery) {
                vstep.setUseMultiQuery(true);
            }
        }
    }

    private static final Set<Class<? extends ProviderOptimizationStrategy>> PRIORS = Collections.singleton(AdjacentVertexFilterOptimizerStrategy.class);


    @Override
    public Set<Class<? extends ProviderOptimizationStrategy>> applyPrior() {
        return PRIORS;
    }

    public static JanusLocalQueryOptimizerStrategy instance() {
        return INSTANCE;
    }
}
