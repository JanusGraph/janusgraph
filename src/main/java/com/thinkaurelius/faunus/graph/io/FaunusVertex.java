package com.thinkaurelius.faunus.graph.io;

import com.tinkerpop.blueprints.pgm.Edge;
import com.tinkerpop.blueprints.pgm.Vertex;
import com.tinkerpop.blueprints.pgm.impls.StringFactory;
import edu.umd.cloud9.io.array.ArrayListOfLongsWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class FaunusVertex extends FaunusElement<Vertex> implements Vertex {

    private List<Edge> outEdges = new LinkedList<Edge>();
    private List<Edge> inEdges = new LinkedList<Edge>();

    public FaunusVertex() {
        super(-1l);
    }

    public FaunusVertex(final long id) {
        super(id);
    }

    public Iterable<Edge> getOutEdges(final String... labels) {
        return outEdges;
    }

    public Iterable<Edge> getInEdges(final String... labels) {
        return inEdges;
    }


    public void write(DataOutput out) throws IOException {
        out.writeByte(Type.VERTEX.val);
        out.writeLong(this.id);
        final ArrayListOfLongsWritable outIds = new ArrayListOfLongsWritable();
        for (final Edge edge : this.getOutEdges()) {
            outIds.add((Long) edge.getId());
        }
        final ArrayListOfLongsWritable inIds = new ArrayListOfLongsWritable();
        for (final Edge edge : this.getInEdges()) {
            inIds.add((Long) edge.getId());
        }
        outIds.write(out);
        inIds.write(out);
    }

    public void readFields(DataInput in) throws IOException {
        byte type = in.readByte();
        this.id = in.readLong();
        final ArrayListOfLongsWritable outIds = new ArrayListOfLongsWritable();
        final ArrayListOfLongsWritable inIds = new ArrayListOfLongsWritable();
        outIds.readFields(in);
        inIds.readFields(in);

        for (long edgeId : outIds) {
            this.outEdges.add(new FaunusEdge(edgeId));
        }

        for (long edgeId : inIds) {
            this.inEdges.add(new FaunusEdge(edgeId));
        }
    }

    public String toString() {
        return StringFactory.vertexString(this);
    }

}
