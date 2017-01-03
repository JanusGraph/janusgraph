package org.janusgraph.graphdb.tinkerpop.optimize;

import org.janusgraph.core.JanusTransaction;
import org.janusgraph.core.JanusVertex;
import org.janusgraph.graphdb.olap.computer.FulgoraElementTraversal;
import org.janusgraph.graphdb.tinkerpop.JanusBlueprintsGraph;
import org.janusgraph.graphdb.transaction.StandardJanusTx;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.IdentityStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.wrapped.WrappedVertex;

import java.util.Optional;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class JanusTraversalUtil {

    public static JanusVertex getJanusVertex(Element v) {
        while (v instanceof WrappedVertex) {
            v = ((WrappedVertex<Vertex>) v).getBaseVertex();
        }
        if (v instanceof JanusVertex) {
            return (JanusVertex) v;
        } else throw new IllegalArgumentException("Expected traverser of Janus vertex but found: " + v);
    }

    public static JanusVertex getJanusVertex(Traverser<? extends Element> traverser) {
        return getJanusVertex(traverser.get());
    }

    public static boolean isEdgeReturnStep(JanusVertexStep vstep) {
        return Edge.class.isAssignableFrom(vstep.getReturnClass());
    }

    public static boolean isVertexReturnStep(JanusVertexStep vstep) {
        return Vertex.class.isAssignableFrom(vstep.getReturnClass());
    }

    public static Step getNextNonIdentityStep(final Step start) {
        Step currentStep = start.getNextStep();
        //Skip over identity steps
        while (currentStep instanceof IdentityStep) currentStep = currentStep.getNextStep();
        return currentStep;
    }

    public static JanusTransaction getTx(Traversal.Admin<?, ?> traversal) {
        JanusTransaction tx = null;
        Optional<Graph> optGraph = TraversalHelper.getRootTraversal(traversal.asAdmin()).getGraph();

        if (traversal instanceof FulgoraElementTraversal) {
            tx = (JanusTransaction) optGraph.get();
        } else {
            if (!optGraph.isPresent())
                throw new IllegalArgumentException("Traversal is not bound to a graph: " + traversal);
            Graph graph = optGraph.get();
            if (graph instanceof JanusTransaction) tx = (JanusTransaction) graph;
            else if (graph instanceof JanusBlueprintsGraph) tx = ((JanusBlueprintsGraph) graph).getCurrentThreadTx();
            else throw new IllegalArgumentException("Traversal is not bound to a Janus Graph, but: " + graph);
        }
        if (tx == null)
            throw new IllegalArgumentException("Not a valid start step for a Janus traversal: " + traversal);
        if (tx.isOpen()) return tx;
        else return ((StandardJanusTx) tx).getNextTx();
    }

}
