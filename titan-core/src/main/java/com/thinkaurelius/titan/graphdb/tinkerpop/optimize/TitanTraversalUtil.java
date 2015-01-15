package com.thinkaurelius.titan.graphdb.tinkerpop.optimize;

import com.google.common.collect.ImmutableSet;
import com.thinkaurelius.titan.core.TitanElement;
import com.thinkaurelius.titan.core.TitanTransaction;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.graphdb.olap.computer.FulgoraElementTraversal;
import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalStrategy;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.graph.step.map.PropertiesStep;
import com.tinkerpop.gremlin.process.graph.step.map.VertexStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.GraphStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.IdentityStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.StartStep;
import com.tinkerpop.gremlin.process.graph.strategy.IdentityRemovalStrategy;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.wrapped.WrappedVertex;

import java.util.Set;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class TitanTraversalUtil {

    static final Set<Class<? extends TraversalStrategy>> POSTS = ImmutableSet.<Class<? extends TraversalStrategy>>of(
            );
    static final Set<Class<? extends TraversalStrategy>> PRIORS = ImmutableSet.<Class<? extends TraversalStrategy>>of(
            IdentityRemovalStrategy.class);

    public static TitanVertex getTitanVertex(Traverser<Vertex> traverser) {
        Vertex v = traverser.get();
        while (v instanceof WrappedVertex) {
            v = ((WrappedVertex<Vertex>)v).getBaseVertex();
        }
        return (TitanVertex)v;
    }

    public static boolean isEdgeReturnStep(VertexStep vstep) {
        return Edge.class.isAssignableFrom(vstep.getReturnClass());
    }

    public static boolean isVertexReturnStep(VertexStep vstep) {
        return Vertex.class.isAssignableFrom(vstep.getReturnClass());
    }

    public static Step getNextNonIdentityStep(final Step start) {
        Step currentStep = start.getNextStep();
        //Skip over identity steps
        while (currentStep instanceof IdentityStep) currentStep = currentStep.getNextStep();
        return currentStep;
    }

    public static TitanTransaction getTx(Traversal traversal) {
        if (traversal instanceof FulgoraElementTraversal) return ((FulgoraElementTraversal)traversal).getGraph();

        Step startStep = TraversalHelper.getStart(traversal);
        if (startStep instanceof GraphStep) {
            Graph graph = ((GraphStep)startStep).getGraph(Graph.class);
            if (graph instanceof TitanTransaction) return (TitanTransaction)graph;
            else throw new IllegalArgumentException("Not a valid Titan traversal ["+graph.getClass()+"]: " + traversal);
        }
        if (startStep instanceof StartStep) {
            Element element = (Element)((StartStep)startStep).getStart();
            if (element instanceof TitanElement) return ((TitanElement)element).graph();
            else throw new IllegalArgumentException("Not a valid Titan traversal because starting element is ["+element+"]: " + traversal);
        }
        throw new IllegalArgumentException("Not a valid start step for a Titan traversal: " + traversal);
    }

}
