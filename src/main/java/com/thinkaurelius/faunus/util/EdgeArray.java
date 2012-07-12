package com.thinkaurelius.faunus.util;

import com.thinkaurelius.faunus.FaunusEdge;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class EdgeArray {

    public static void write(final List<FaunusEdge> edges, final DataOutput out) throws IOException {
        out.writeInt(edges.size());
        for (final FaunusEdge edge : edges) {
            edge.write(out);
        }
    }

    public static List<FaunusEdge> readFields(final DataInput in) throws IOException {
        final List<FaunusEdge> edges = new ArrayList<FaunusEdge>();
        final int size = in.readInt();
        for (int i = 0; i < size; i++) {
            edges.add(new FaunusEdge(in));
        }
        return edges;
    }
}
