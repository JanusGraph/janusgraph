package com.tinkerpop.furnace.alpha.generators;

import java.util.Map;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Vertex;

/**
 * Base class for all synthetic network generators.
 * 
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public abstract class AbstractGenerator {

    private final String label;
    private final EdgeAnnotator edgeAnnotator;
    private final VertexAnnotator vertexAnnotator;

    /**
     * Constructs a new network generator for edges with the given label and annotator.
     *
     * @param label Label for the generated edges
     * @param edgeAnnotator EdgeAnnotator to use for annotating newly generated edges.
     * @param vertexAnnotator VertexAnnotator to use for annotating process vertices.
     */
    public AbstractGenerator(String label, EdgeAnnotator edgeAnnotator, VertexAnnotator vertexAnnotator) {
        if (label==null || label.isEmpty()) throw new IllegalArgumentException("Label cannot be empty");
        if (edgeAnnotator==null) throw new NullPointerException();
        if (vertexAnnotator==null) throw new NullPointerException();
        this.label = label;
        this.edgeAnnotator = edgeAnnotator;
        this.vertexAnnotator = vertexAnnotator;
    }

    /**
     * Constructs a new network generator for edges with the given label and annotator.
     *
     * @param label Label for the generated edges
     * @param annotator EdgeAnnotator to use for annotating newly generated edges.
     */
    public AbstractGenerator(String label, EdgeAnnotator annotator) {
        this(label, annotator, VertexAnnotator.NONE);
    }

    /**
     * Constructs a new network generator for edges with the given label and an empty annotator.
     *
     * @param label Label for the generated edges
     */
    public AbstractGenerator(String label) {
        this(label,EdgeAnnotator.NONE);
    }

    /**
     * Returns the label for this generator.
     * 
     * @return
     */
    public final String getLabel() {
        return label;
    }

    /**
     * Returns the {@link EdgeAnnotator} for this generator
     * @return
     */
    public final EdgeAnnotator getEdgeAnnotator() {
        return edgeAnnotator;
    }

    /**
     * Returns the {@link VertexAnnotator} for this generator
     * @return
     */
    public final VertexAnnotator getVertexAnnotator() {
        return vertexAnnotator;
    }
    
    protected final Edge addEdge(Graph graph, Vertex out, Vertex in) {
        Edge e = graph.addEdge(null,out,in,label);
        edgeAnnotator.annotate(e);
        return e;
    }

    protected final Vertex processVertex(Vertex vertex, Map<String, Object> context) {
        vertexAnnotator.annotate(vertex, context);
        return vertex;
    }

}
