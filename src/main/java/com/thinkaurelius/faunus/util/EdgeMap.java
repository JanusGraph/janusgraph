package com.thinkaurelius.faunus.util;

import com.thinkaurelius.faunus.FaunusEdge;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class EdgeMap {


    public static Map<String, List<FaunusEdge>> readFields(final DataInput in) throws IOException {
        final Map<String, List<FaunusEdge>> edges = new HashMap<String, List<FaunusEdge>>();
        int edgeTypes = in.readShort();
        for (int i = 0; i < edgeTypes; i++) {
            final String label = in.readUTF();
            final int size = in.readInt();
            final List<FaunusEdge> temp = new ArrayList<FaunusEdge>(size);
            for (int j = 0; j < size; j++) {
                temp.add(new FaunusEdge(in));
            }
            edges.put(label, temp);
        }
        return edges;
    }

    public static void write(final Map<String, List<FaunusEdge>> edges, final DataOutput out) throws IOException {
        out.writeShort(edges.size());
        for (final Map.Entry<String, List<FaunusEdge>> entry : edges.entrySet()) {
            out.writeUTF(entry.getKey());
            out.writeInt(entry.getValue().size());
            for (final FaunusEdge edge : entry.getValue()) {
                edge.write(out);
            }
        }
    }


}
