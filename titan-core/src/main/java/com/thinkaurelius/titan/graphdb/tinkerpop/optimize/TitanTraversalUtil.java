package com.thinkaurelius.titan.graphdb.tinkerpop.optimize;

import com.google.common.collect.ImmutableSet;
import com.thinkaurelius.titan.core.TitanElement;
import com.thinkaurelius.titan.core.TitanTransaction;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.graphdb.olap.computer.FulgoraElementTraversal;
import com.thinkaurelius.titan.graphdb.tinkerpop.TitanBlueprintsGraph;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertiesStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.IdentityStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.StartStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.IdentityRemovalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.wrapped.WrappedVertex;

import java.util.Optional;
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

    public static TitanTransaction getTx(Traversal.Admin<?,?> traversal) {
        TitanTransaction tx=null;
        Optional<Graph> optGraph = TraversalHelper.getRootTraversal(traversal.asAdmin()).getGraph();

        if (traversal instanceof FulgoraElementTraversal) {
            tx = (TitanTransaction)optGraph.get();
        } else {
            if (!optGraph.isPresent()) throw new IllegalArgumentException("Traversal is not bound to a graph: " + traversal);
            Graph graph = optGraph.get();
            if (graph instanceof TitanTransaction) tx = (TitanTransaction)graph;
            else if (graph instanceof TitanBlueprintsGraph) tx = ((TitanBlueprintsGraph)graph).getCurrentThreadTx();
            else throw new IllegalArgumentException("Traversal is not bound to a Titan Graph, but: " + graph);
        }
        if (tx==null) throw new IllegalArgumentException("Not a valid start step for a Titan traversal: " + traversal);
        if (tx.isOpen()) return tx;
        else return ((StandardTitanTx)tx).getNextTx();
    }

}
