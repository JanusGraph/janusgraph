package com.thinkaurelius.titan.graphdb.olap.computer;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.TitanTransaction;
import com.thinkaurelius.titan.graphdb.tinkerpop.optimize.TitanElementStepStrategy;
import com.thinkaurelius.titan.graphdb.tinkerpop.optimize.TitanLocalQueryOptimizerStrategy;
import com.thinkaurelius.titan.graphdb.tinkerpop.optimize.TitanTraversalUtil;
import com.thinkaurelius.titan.graphdb.tinkerpop.optimize.TitanVertexStep;
import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.TraversalStrategies;
import com.tinkerpop.gremlin.process.computer.MessageCombiner;
import com.tinkerpop.gremlin.process.computer.MessageScope;
import com.tinkerpop.gremlin.process.computer.VertexProgram;
import com.tinkerpop.gremlin.process.graph.step.filter.FilterStep;
import com.tinkerpop.gremlin.process.graph.step.filter.RangeStep;
import com.tinkerpop.gremlin.process.graph.step.map.OrderStep;
import com.tinkerpop.gremlin.process.graph.step.map.VertexStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.IdentityStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.StartStep;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Vertex;

import java.util.List;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class FulgoraUtil {

    private static TraversalStrategies FULGORA_STRATEGIES;

    static {
        try {
            FULGORA_STRATEGIES = TraversalStrategies.GlobalCache.getStrategies(Vertex.class).clone().addStrategies(TitanLocalQueryOptimizerStrategy.instance());
        } catch (final CloneNotSupportedException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    public static Traversal<Vertex,Edge> getTraversal(final MessageScope.Local<?> scope,
                                                                       final TitanTransaction graph) {
        Traversal<Vertex,Edge> incident = scope.getIncidentTraversal().get();
        FulgoraElementTraversal<Vertex,Edge> result = FulgoraElementTraversal.of(graph);
        for (Step step : incident.asAdmin().getSteps()) result.addStep(step);
        result.asAdmin().setStrategies(FULGORA_STRATEGIES);
        result.asAdmin().applyStrategies(TraversalEngine.COMPUTER);
        verifyIncidentTraversal(result);
        return result;
    }

    public static Traversal<Vertex,Edge> getReverseElementTraversal(final MessageScope.Local<?> scope,
                                                                       final Vertex start,
                                                                       final TitanTransaction graph) {
        Traversal<Vertex,Edge> incident = scope.getIncidentTraversal().get();
        Step<Vertex,?> startStep = TraversalHelper.getStart(incident.asAdmin());
        assert startStep instanceof VertexStep;
        ((VertexStep) startStep).reverse();

        incident.asAdmin().addStep(0,new StartStep<>(incident,start));
        incident.asAdmin().setStrategies(FULGORA_STRATEGIES);
        return incident;
    }

    private static void verifyIncidentTraversal(FulgoraElementTraversal<Vertex,Edge> traversal) {
        //First step must be TitanVertexStep
        List<Step> steps = traversal.getSteps();
        Step<Vertex,?> startStep = TraversalHelper.getStart(traversal);
        Preconditions.checkArgument(startStep instanceof TitanVertexStep &&
                TitanTraversalUtil.isEdgeReturnStep((TitanVertexStep) startStep),"Expected first step to be an edge step but found: %s",startStep);
        for (int i = 1; i < steps.size(); i++) {
            Step step = steps.get(i);
            if (step instanceof OrderStep ||
                step instanceof IdentityStep ||
                step instanceof FilterStep ||
                step instanceof RangeStep) continue;
            throw new IllegalArgumentException("Encountered unsupported step in incident traversal: " + step);
        }
    }

    public static<M> MessageCombiner<M> getMessageCombiner(VertexProgram<M> program) {
        return program.getMessageCombiner().orElse(DEFAULT_COMBINER);
    }


    private static final MessageCombiner DEFAULT_COMBINER = new ThrowingCombiner<>();

    private static class ThrowingCombiner<M> implements MessageCombiner<M> {

        @Override
        public M combine(M messageA, M messageB) {
            throw new IllegalArgumentException("The VertexProgram needs to define a message combiner in order " +
                    "to preserve memory and handle partitioned vertices");
        }
    }

}
