package com.thinkaurelius.faunus.io.graph.util;

import com.thinkaurelius.faunus.io.graph.FaunusEdge;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class FaunusEdgeArray implements Writable {

    private List<FaunusEdge> edges;

    public FaunusEdgeArray(final List<FaunusEdge> edges) {
        this.edges = edges;
    }

    public FaunusEdgeArray(final DataInput in) throws IOException {
        this.readFields(in);
    }

    public List<FaunusEdge> getEdges() {
        return this.edges;
    }

    public void write(final DataOutput out) throws IOException {
        out.writeInt(this.edges.size());
        for (final FaunusEdge edge : this.edges) {
            edge.write(out);
        }
    }

    public void readFields(final DataInput in) throws IOException {
        this.edges = new LinkedList<FaunusEdge>();
        final int size = in.readInt();
        for (int i = 0; i < size; i++) {
            this.edges.add(new FaunusEdge(in));
        }
    }
}
