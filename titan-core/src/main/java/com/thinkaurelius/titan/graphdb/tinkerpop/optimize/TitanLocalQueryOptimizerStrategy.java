package com.thinkaurelius.titan.graphdb.tinkerpop.optimize;

import com.thinkaurelius.titan.graphdb.query.QueryUtil;
import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.TraversalStrategy;
import com.tinkerpop.gremlin.process.graph.step.filter.FilterStep;
import com.tinkerpop.gremlin.process.graph.step.filter.LocalRangeStep;
import com.tinkerpop.gremlin.process.graph.step.filter.RangeStep;
import com.tinkerpop.gremlin.process.graph.step.map.EdgeVertexStep;
import com.tinkerpop.gremlin.process.graph.step.map.OrderByStep;
import com.tinkerpop.gremlin.process.graph.step.map.OrderStep;
import com.tinkerpop.gremlin.process.graph.step.map.VertexStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.GraphStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.IdentityStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.SideEffectStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.StartStep;
import com.tinkerpop.gremlin.process.graph.strategy.LocalRangeStrategy;
import com.tinkerpop.gremlin.process.util.EmptyStep;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Vertex;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TitanLocalQueryOptimizerStrategy implements TraversalStrategy {

    private static final TitanLocalQueryOptimizerStrategy INSTANCE = new TitanLocalQueryOptimizerStrategy();

    private TitanLocalQueryOptimizerStrategy() {
    }

    @Override
    public void apply(final Traversal<?, ?> traversal, final TraversalEngine engine) {
        if (engine.equals(TraversalEngine.COMPUTER))
            return;

        TraversalHelper.getStepsOfClass(TitanVertexStep.class, traversal).forEach(step -> {
            if (step.isEdgeStep()) {
                HasStepFolder.foldInHasContainer(step,traversal);
                OrderByStep ostep = HasStepFolder.foldInLastOrderBy(step,traversal,false);
                boolean hasLocalRange = HasStepFolder.foldInRange(step, traversal, LocalRangeStep.class);
                if (ostep!=null && !hasLocalRange) TraversalHelper.insertAfterStep(ostep,step,traversal);
            } else {
                assert Vertex.class.isAssignableFrom(step.getReturnClass());
                if (step.getNextStep() instanceof RangeStep) {
                    //If its a global limit, then each local limit should be at least as much. But don't remove since it is global!
                    RangeStep rstep = (RangeStep)step.getNextStep();
                    int limit = QueryUtil.convertLimit(rstep.getHighRange());
                    step.setLimit(QueryUtil.mergeLimits(limit, step.getLimit()));
                }
            }
        });

        TraversalHelper.getStepsOfClass(TitanPropertiesStep.class, traversal).forEach(step -> {
            //Determine if this step can only ever encounter vertices
            boolean isVertexProperties = false;
            Step previousStep = step;
            while ((previousStep=previousStep.getPreviousStep())!= EmptyStep.instance()) {
                if (previousStep instanceof FilterStep || previousStep instanceof OrderStep ||
                        previousStep instanceof OrderByStep ||
                        previousStep instanceof IdentityStep) {
                    continue; //we can skip over those since they don't alter the element type
                }

                if (previousStep instanceof VertexStep) {
                    if (Vertex.class.isAssignableFrom(((VertexStep) previousStep).getReturnClass()))
                        isVertexProperties = true;
                } else if (previousStep instanceof EdgeVertexStep) {
                    isVertexProperties = true;
                } else if (previousStep instanceof GraphStep) {
                    if (((GraphStep)previousStep).returnsVertices()) isVertexProperties=true;
                } else if (previousStep instanceof StartStep) {
                    if (((StartStep)previousStep).startAssignableTo(Vertex.class)) isVertexProperties = true;
                }
                break;

            }

            if (isVertexProperties) {
                step.makeVertrexProperties();
                HasStepFolder.foldInHasContainer(step,traversal);
                OrderByStep ostep = HasStepFolder.foldInLastOrderBy(step,traversal,false);
                boolean hasLocalRange = HasStepFolder.foldInRange(step, traversal, LocalRangeStep.class);
                if (ostep!=null && !hasLocalRange) TraversalHelper.insertAfterStep(ostep,step,traversal);
            }
        });
    }

    public static TitanLocalQueryOptimizerStrategy instance() {
        return INSTANCE;
    }

    @Override
    public int compareTo(TraversalStrategy ts) {
        return -1;
    }

}
