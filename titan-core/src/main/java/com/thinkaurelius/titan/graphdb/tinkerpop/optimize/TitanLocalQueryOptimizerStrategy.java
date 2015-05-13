package com.thinkaurelius.titan.graphdb.tinkerpop.optimize;

import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import com.thinkaurelius.titan.graphdb.query.QueryUtil;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.LocalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.AdjacentToIncidentStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.IdentityRemovalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.IncidentToAdjacentStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Matthias Broecheler (http://matthiasb.com)
 */
public class TitanLocalQueryOptimizerStrategy extends AbstractTraversalStrategy<TraversalStrategy.OptimizationStrategy> implements TraversalStrategy.OptimizationStrategy {

    private static final TitanLocalQueryOptimizerStrategy INSTANCE = new TitanLocalQueryOptimizerStrategy();

    private TitanLocalQueryOptimizerStrategy() {
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        if (!traversal.getGraph().isPresent())
            return;

        Graph graph = traversal.getGraph().get();

        //If this is a compute graph then we can't apply local traversal optimisation at this stage.
        if (!(graph instanceof EmptyGraph)) {
            StandardTitanGraph titanGraph = graph instanceof StandardTitanTx ? ((StandardTitanTx) graph).getGraph() : (StandardTitanGraph) graph;

            TraversalHelper.getStepsOfClass(VertexStep.class, traversal).forEach(originalStep -> {
                TitanVertexStep vstep = new TitanVertexStep(originalStep);
                TraversalHelper.replaceStep(originalStep, vstep, traversal);


                if (TitanTraversalUtil.isEdgeReturnStep(vstep)) {
                    HasStepFolder.foldInHasContainer(vstep, traversal);
                    //We cannot fold in orders or ranges since they are not local
                }

                assert TitanTraversalUtil.isEdgeReturnStep(vstep) || TitanTraversalUtil.isVertexReturnStep(vstep);
                Step nextStep = TitanTraversalUtil.getNextNonIdentityStep(vstep);
                if (nextStep instanceof RangeGlobalStep) {
                    int limit = QueryUtil.convertLimit(((RangeGlobalStep) nextStep).getHighRange());
                    vstep.setLimit(QueryUtil.mergeLimits(limit, vstep.getLimit()));
                }

                if (traversal.getEngine().isStandard() &&
                        titanGraph.getConfiguration().useMultiQuery()) {
                    vstep.setUseMultiQuery(true);
                }
            });

//        TraversalHelper.getStepsOfClass(TitanPropertiesStep.class, traversal).forEach(step -> {
//            //Determine if this step can only ever encounter vertices
//            boolean isVertexProperties = false;
//            Step previousStep = step;
//            while ((previousStep=previousStep.getPreviousStep())!= EmptyStep.instance()) {
//                if (previousStep instanceof FilterStep || previousStep instanceof OrderStep ||
//                        previousStep instanceof OrderByStep ||
//                        previousStep instanceof IdentityStep) {
//                    continue; //we can skip over those since they don't alter the element type
//                }
//
//                if (previousStep instanceof VertexStep) {
//                    if (Vertex.class.isAssignableFrom(((VertexStep) previousStep).getReturnClass()))
//                        isVertexProperties = true;
//                } else if (previousStep instanceof EdgeVertexStep) {
//                    isVertexProperties = true;
//                } else if (previousStep instanceof GraphStep) {
//                    if (((GraphStep)previousStep).returnsVertices()) isVertexProperties=true;
//                } else if (previousStep instanceof StartStep) {
//                    if (((StartStep)previousStep).startAssignableTo(Vertex.class)) isVertexProperties = true;
//                }
//                break;
//
//            }
//
//            if (isVertexProperties) {
//                step.makeVertrexProperties();
//                HasStepFolder.foldInHasContainer(step,traversal);
//                OrderByStep ostep = HasStepFolder.foldInLastOrderBy(step,traversal,false);
//                boolean hasLocalRange = HasStepFolder.foldInRange(step, traversal, LocalRangeStep.class);
//                if (ostep!=null && !hasLocalRange) TraversalHelper.insertAfterStep(ostep,step,traversal);
//
//                if (engine.equals(TraversalEngine.STANDARD) &&
//                        ((StandardTitanTx)traversal.sideEffects().getGraph()).getGraph().getConfiguration().useMultiQuery()) {
//                    step.setUseMultiQuery(true);
//                }
//            }
//        });

            TraversalHelper.getStepsOfClass(LocalStep.class, traversal).forEach(localStep -> {
                Traversal.Admin localTraversal = ((LocalStep<?, ?>) localStep).getLocalChildren().get(0);

                Step localStart = localTraversal.getStartStep();
                if (localStart instanceof VertexStep) {
                    TitanVertexStep vstep = new TitanVertexStep((VertexStep) localStart);
                    TraversalHelper.replaceStep(localStart, vstep, localTraversal);

                    if (TitanTraversalUtil.isEdgeReturnStep(vstep)) {
                        HasStepFolder.foldInHasContainer(vstep, localTraversal);
                        HasStepFolder.foldInOrder(vstep, localTraversal, traversal, false);
                    }
                    HasStepFolder.foldInRange(vstep, localTraversal);


                    assert localTraversal.asAdmin().getSteps().size() > 0;
                    if (localTraversal.asAdmin().getSteps().size() == 1) {
                        //Can replace the entire localStep by the vertex step in the outer traversal
                        assert localTraversal.getStartStep() == vstep;
                        vstep.setTraversal(traversal);
                        TraversalHelper.replaceStep(localStep, vstep, traversal);

                        if (traversal.getEngine().isStandard() &&
                                titanGraph.getConfiguration().useMultiQuery()) {
                            vstep.setUseMultiQuery(true);
                        }
                    }
                }
            });
        }
    }

    public static TitanLocalQueryOptimizerStrategy instance() {
        return INSTANCE;
    }

    @Override
    public Set<Class<? extends OptimizationStrategy>> applyPrior() {
        return new HashSet<>(Arrays.asList(AdjacentToIncidentStrategy.class, IncidentToAdjacentStrategy.class, IdentityRemovalStrategy.class));
    }

    @Override
    public Set<Class<? extends OptimizationStrategy>> applyPost() {
        return TitanTraversalUtil.POSTS;
    }


}
