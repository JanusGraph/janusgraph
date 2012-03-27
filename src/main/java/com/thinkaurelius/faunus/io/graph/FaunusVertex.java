package com.thinkaurelius.faunus.io.graph;

import com.thinkaurelius.faunus.io.graph.util.FaunusEdgeArray;
import com.tinkerpop.blueprints.pgm.Edge;
import com.tinkerpop.blueprints.pgm.Vertex;
import com.tinkerpop.blueprints.pgm.impls.StringFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class FaunusVertex extends FaunusElement<Vertex> implements Vertex {

    private List<Edge> outEdges = new LinkedList<Edge>();

    public FaunusVertex() {
        super(-1l);
    }

    public FaunusVertex(final long id) {
        super(id);
    }

    public FaunusVertex(final FaunusVertex vertex) {
        super((Long) vertex.getId());
        this.outEdges = (List<Edge>) vertex.getOutEdges();
        this.setProperties(vertex.getProperties());
    }

    public FaunusVertex(final DataInput in) throws IOException {
        super(-1l);
        this.readFields(in);
    }

    public Iterable<Edge> getOutEdges(final String... labels) {
        if (null != labels && labels.length > 0) {
            final Set<String> legalLabels = new HashSet<String>(Arrays.asList(labels));
            final LinkedList<Edge> filteredEdges = new LinkedList<Edge>();
            for (final Edge edge : this.outEdges) {
                if (legalLabels.contains(edge.getLabel())) {
                    filteredEdges.add(edge);
                }
            }
            return filteredEdges;
        } else {
            return this.outEdges;
        }
    }

    public Iterable<Edge> getInEdges(final String... labels) {
        throw new UnsupportedOperationException("To reduce data redundancy, this operation is not supported");
    }

    public void addOutEdge(final FaunusEdge edge) {
        this.outEdges.add(edge);
    }

    public void setOutEdges(final List<Edge> outEdges) {
        this.outEdges = outEdges;
    }


    public void write(final DataOutput out) throws IOException {
        super.write(out);
        new FaunusEdgeArray((List) this.outEdges).write(out);
    }

    public void readFields(final DataInput in) throws IOException {
        super.readFields(in);
        this.outEdges = (List) new FaunusEdgeArray(in).getEdges();
    }

    public String toString() {
        return StringFactory.vertexString(this);
    }

}
