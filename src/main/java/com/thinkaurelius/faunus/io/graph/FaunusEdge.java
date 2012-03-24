package com.thinkaurelius.faunus.io.graph;

import com.tinkerpop.blueprints.pgm.Edge;
import com.tinkerpop.blueprints.pgm.Vertex;
import com.tinkerpop.blueprints.pgm.impls.StringFactory;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class FaunusEdge extends FaunusElement<Edge> implements Edge, Writable {

    private FaunusVertex outVertex;
    private FaunusVertex inVertex;
    private String label = "_default";

    public FaunusEdge() {
        super(-1l);
    }

    public FaunusEdge(final DataInput in) throws IOException {
        super(-1l);
        this.readFields(in);
    }

    public FaunusEdge(final FaunusEdge edge) throws IOException {
        super((Long) edge.getId());
        this.outVertex = (FaunusVertex) edge.getOutVertex();
        this.inVertex = (FaunusVertex) edge.getInVertex();
        this.label = edge.getLabel();
        this.properties = new HashMap<String, Object>();
        for (String key : edge.getPropertyKeys()) {
            this.properties.put(key, edge.getProperty(key));
        }
    }

    public FaunusEdge(final long id, final FaunusVertex outVertex, final FaunusVertex inVertex, final String label) {
        super(id);
        this.outVertex = outVertex;
        this.inVertex = inVertex;
        this.label = label;
    }

    public Vertex getOutVertex() {
        return outVertex;
    }

    public Vertex getInVertex() {
        return inVertex;
    }

    public String getLabel() {
        return label;
    }

    public void write(final DataOutput out) throws IOException {
        super.write(out);
        out.writeLong((Long) this.getInVertex().getId());
        out.writeLong((Long) this.getOutVertex().getId());
        out.writeUTF(this.getLabel());
    }

    public void readFields(final DataInput in) throws IOException {
        super.readFields(in);
        this.inVertex = new FaunusVertex(in.readLong());
        this.outVertex = new FaunusVertex(in.readLong());
        this.label = in.readUTF();
    }

    public String toString() {
        return StringFactory.edgeString(this);
    }
}
