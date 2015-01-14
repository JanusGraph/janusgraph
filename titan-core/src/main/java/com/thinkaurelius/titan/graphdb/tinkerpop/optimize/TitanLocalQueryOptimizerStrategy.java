package com.thinkaurelius.titan.graphdb.tinkerpop.optimize;

import com.thinkaurelius.titan.graphdb.query.QueryUtil;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.TraversalStrategy;
import com.tinkerpop.gremlin.process.graph.step.filter.RangeStep;
import com.tinkerpop.gremlin.process.graph.step.map.LocalStep;
import com.tinkerpop.gremlin.process.graph.step.map.VertexStep;
import com.tinkerpop.gremlin.process.graph.strategy.AbstractTraversalStrategy;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Matthias Broecheler (http://matthiasb.com)
 */
public class TitanLocalQueryOptimizerStrategy extends AbstractTraversalStrategy {

    private static final TitanLocalQueryOptimizerStrategy INSTANCE = new TitanLocalQueryOptimizerStrategy();

    private TitanLocalQueryOptimizerStrategy() {
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal, final TraversalEngine engine) {
        TraversalHelper.getStepsOfClass(VertexStep.class, traversal).forEach(originalStep -> {
            TitanVertexStep vstep = new TitanVertexStep(originalStep);
            TraversalHelper.replaceStep(originalStep,vstep,traversal);


            if (TitanTraversalUtil.isEdgeReturnStep(vstep)) {
                HasStepFolder.foldInHasContainer(vstep,traversal);
                //We cannot fold in orders or ranges since they are not local
            }

            assert TitanTraversalUtil.isEdgeReturnStep(vstep) || TitanTraversalUtil.isVertexReturnStep(vstep);
            Step nextStep = TitanTraversalUtil.getNextNonIdentityStep(vstep);
            if (nextStep instanceof RangeStep) {
                int limit = QueryUtil.convertLimit(((RangeStep)nextStep).getHighRange());
                vstep.setLimit(QueryUtil.mergeLimits(limit, vstep.getLimit()));
            }

            if (engine.equals(TraversalEngine.STANDARD) &&
                    ((StandardTitanTx)TitanTraversalUtil.getTx(traversal)).getGraph().getConfiguration().useMultiQuery()) {
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
            Traversal.Admin localTraversal = ((Traversal)localStep.getTraversals().get(0)).asAdmin();

            Step localStart = TraversalHelper.getStart(localTraversal);
            if (localStart instanceof VertexStep) {
                TitanVertexStep vstep = new TitanVertexStep((VertexStep)localStart);
                TraversalHelper.replaceStep(localStart,vstep,localTraversal);

                if (TitanTraversalUtil.isEdgeReturnStep(vstep)) {
                    HasStepFolder.foldInHasContainer(vstep,localTraversal);
                    HasStepFolder.foldInOrder(vstep, localTraversal, false);
                }
                HasStepFolder.foldInRange(vstep, localTraversal);

                if (engine.equals(TraversalEngine.STANDARD) &&
                        ((StandardTitanTx)TitanTraversalUtil.getTx(traversal)).getGraph().getConfiguration().useMultiQuery()) {
                    vstep.setUseMultiQuery(true);
                }

                assert localTraversal.asAdmin().getSteps().size()>0;
                if (localTraversal.asAdmin().getSteps().size()==1) {
                    //Can replace the entire localStep by the vertex step in the outer traversal
                    assert TraversalHelper.getStart(localTraversal)==vstep;
                    TraversalHelper.replaceStep(localStep,vstep,traversal);
                }
            }
        });
    }

    public static TitanLocalQueryOptimizerStrategy instance() {
        return INSTANCE;
    }

    @Override
    public Set<Class<? extends TraversalStrategy>> applyPrior() {
        return TitanTraversalUtil.PRIORS;
    }

    @Override
    public Set<Class<? extends TraversalStrategy>> applyPost() {
        return TitanTraversalUtil.POSTS;
    }


}
