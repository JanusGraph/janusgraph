package org.janusgraph.graphdb.olap.computer;

import com.google.common.base.Preconditions;
import org.janusgraph.core.JanusTransaction;
import org.janusgraph.graphdb.tinkerpop.optimize.JanusLocalQueryOptimizerStrategy;
import org.janusgraph.graphdb.tinkerpop.optimize.JanusTraversalUtil;
import org.janusgraph.graphdb.tinkerpop.optimize.JanusVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.computer.MessageCombiner;
import org.apache.tinkerpop.gremlin.process.computer.MessageScope;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.FilterStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderLocalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.IdentityStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.StartStep;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Optional;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class FulgoraUtil {

    private final static TraversalStrategies FULGORA_STRATEGIES = TraversalStrategies.GlobalCache.getStrategies(Graph.class).clone().addStrategies(JanusLocalQueryOptimizerStrategy.instance());;

    public static JanusVertexStep<Vertex> getReverseJanusVertexStep(final MessageScope.Local<?> scope,
                                                                       final JanusTransaction graph) {
        FulgoraElementTraversal<Vertex,Edge> result = getReverseTraversal(scope,graph,null);
        result.asAdmin().applyStrategies();
        verifyIncidentTraversal(result);
        return (JanusVertexStep)result.getStartStep();
    }

    public static Traversal<Vertex,Edge> getReverseElementTraversal(final MessageScope.Local<?> scope,
                                                                    final Vertex start,
                                                                    final JanusTransaction graph) {
        return getReverseTraversal(scope,graph,start);
    }

    private static FulgoraElementTraversal<Vertex,Edge> getReverseTraversal(final MessageScope.Local<?> scope,
                                                      final JanusTransaction graph, @Nullable final Vertex start) {
        Traversal.Admin<Vertex,Edge> incident = scope.getIncidentTraversal().get().asAdmin();
        FulgoraElementTraversal<Vertex,Edge> result = FulgoraElementTraversal.of(graph);

        for (Step step : incident.getSteps()) result.addStep(step);
        Step<Vertex,?> startStep = result.getStartStep();
        assert startStep instanceof VertexStep;
        ((VertexStep) startStep).reverseDirection();

        if (start!=null) result.addStep(0, new StartStep<>(incident, start));
        result.asAdmin().setStrategies(FULGORA_STRATEGIES);
        return result;
    }


    private static void verifyIncidentTraversal(FulgoraElementTraversal<Vertex,Edge> traversal) {
        //First step must be JanusVertexStep
        List<Step> steps = traversal.getSteps();
        Step<Vertex,?> startStep = steps.get(0);
        Preconditions.checkArgument(startStep instanceof JanusVertexStep &&
                JanusTraversalUtil.isEdgeReturnStep((JanusVertexStep) startStep),"Expected first step to be an edge step but found: %s",startStep);
        Optional<Step> violatingStep = steps.stream().filter(s -> !(s instanceof JanusVertexStep ||
                s instanceof OrderGlobalStep || s instanceof OrderLocalStep ||
                        s instanceof IdentityStep || s instanceof FilterStep)).findAny();
        if (violatingStep.isPresent()) throw new IllegalArgumentException("Encountered unsupported step in incident traversal: " + violatingStep.get());
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
