package com.thinkaurelius.faunus.graph.io;

import com.tinkerpop.blueprints.pgm.Edge;
import com.tinkerpop.blueprints.pgm.Vertex;
import com.tinkerpop.blueprints.pgm.impls.StringFactory;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

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

    public FaunusEdge(final long id) {
        super(id);
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

    public void write(DataOutput out) throws IOException {
        out.writeByte(Type.EDGE.val);
        out.writeLong(this.id);
        out.writeLong((Long) this.getInVertex().getId());
        out.writeLong((Long) this.getOutVertex().getId());
        out.writeUTF(this.getLabel());
    }

    public void readFields(DataInput in) throws IOException {
        byte type = in.readByte();
        this.id = in.readLong();
        this.inVertex = new FaunusVertex(in.readLong());
        this.outVertex = new FaunusVertex(in.readLong());
        this.label = in.readUTF();
    }

    public String toString() {
        return StringFactory.edgeString(this);
    }
}
