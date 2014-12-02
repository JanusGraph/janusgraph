package com.thinkaurelius.titan.graphdb.olap.computer;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.TitanTransaction;
import com.thinkaurelius.titan.graphdb.tinkerpop.optimize.TitanElementTraversal;
import com.thinkaurelius.titan.graphdb.tinkerpop.optimize.TitanVertexStep;
import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.computer.MessageCombiner;
import com.tinkerpop.gremlin.process.computer.MessageScope;
import com.tinkerpop.gremlin.process.computer.VertexProgram;
import com.tinkerpop.gremlin.process.graph.step.filter.FilterStep;
import com.tinkerpop.gremlin.process.graph.step.filter.LocalRangeStep;
import com.tinkerpop.gremlin.process.graph.step.filter.RangeStep;
import com.tinkerpop.gremlin.process.graph.step.map.OrderByStep;
import com.tinkerpop.gremlin.process.graph.step.map.OrderStep;
import com.tinkerpop.gremlin.process.graph.step.map.VertexStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.IdentityStep;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Vertex;

import java.util.List;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class FulgoraUtil {

    public static FulgoraElementTraversal<Vertex,Edge> getTitanTraversal(final MessageScope.Local<?> scope,
                                                                       final TitanTransaction graph) {
        Traversal<Vertex,Edge> incident = scope.getIncidentTraversal().get();
        FulgoraElementTraversal<Vertex,Edge> result = FulgoraElementTraversal.of(graph);
        for (Step step : incident.getSteps()) result.addStep(step);
        verifyIncidentTraversal(result);
        return result;
    }

    public static TitanElementTraversal<Vertex,Edge> getReverseElementTraversal(final MessageScope.Local<?> scope,
                                                                       final Vertex start,
                                                                       final TitanTransaction graph) {
        Traversal<Vertex,Edge> incident = scope.getIncidentTraversal().get();
        TitanElementTraversal<Vertex,Edge> result = new TitanElementTraversal<>(start,graph);
        for (Step step : incident.getSteps()) {
            step.setTraversal(result);
            if (step instanceof VertexStep) ((VertexStep) step).reverse();
            result.addStep(step);
        }
        return result;
    }

    private static void verifyIncidentTraversal(FulgoraElementTraversal<Vertex,Edge> traversal) {
        traversal.applyStrategies(TraversalEngine.COMPUTER);
        //First step must be TitanVertexStep
        List<Step> steps = traversal.getSteps();
        Step<Vertex,?> startStep = TraversalHelper.getStart(traversal);
        Preconditions.checkArgument(startStep instanceof TitanVertexStep &&
                ((TitanVertexStep)startStep).isEdgeStep(),"Expected first step to be an edge step but found: %s",startStep);
        for (int i = 1; i < steps.size(); i++) {
            Step step = steps.get(i);
            if (step instanceof OrderByStep || step instanceof OrderStep ||
                step instanceof IdentityStep ||
                step instanceof FilterStep ||
                step instanceof RangeStep || step instanceof LocalRangeStep) continue;
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
