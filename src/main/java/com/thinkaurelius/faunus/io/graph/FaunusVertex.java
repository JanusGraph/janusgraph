package com.thinkaurelius.faunus.io.graph;

import com.thinkaurelius.faunus.io.graph.util.EdgeArray;
import com.thinkaurelius.faunus.io.graph.util.ElementProperties;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Query;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.DefaultQuery;
import com.tinkerpop.blueprints.util.StringFactory;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class FaunusVertex extends FaunusElement<Vertex> implements Vertex {

    static {
        WritableComparator.define(FaunusVertex.class, new Comparator());
    }

    private List<Edge> outEdges = new ArrayList<Edge>();
    private List<Edge> inEdges = new ArrayList<Edge>();

    public FaunusVertex() {
        super(-1l);
    }

    public FaunusVertex(final long id) {
        super(id);
    }

    public FaunusVertex(final DataInput in) throws IOException {
        super(-1l);
        this.readFields(in);
    }

    public Query query() {
        return new DefaultQuery(this);
    }

    public Iterable<Edge> getEdges(final Direction direction, final String... labels) {
        if (Direction.OUT.equals(direction)) {
            if (null != labels && labels.length > 0) {
                final Set<String> legalLabels = new HashSet<String>(Arrays.asList(labels));
                final List<Edge> filteredEdges = new ArrayList<Edge>();
                for (final Edge edge : this.outEdges) {
                    if (legalLabels.contains(edge.getLabel())) {
                        filteredEdges.add(edge);
                    }
                }
                return filteredEdges;
            } else {
                return this.outEdges;
            }
        } else if (Direction.IN.equals(direction)) {
            if (null != labels && labels.length > 0) {
                final Set<String> legalLabels = new HashSet<String>(Arrays.asList(labels));
                final List<Edge> filteredEdges = new ArrayList<Edge>();
                for (final Edge edge : this.inEdges) {
                    if (legalLabels.contains(edge.getLabel())) {
                        filteredEdges.add(edge);
                    }
                }
                return filteredEdges;
            } else {
                return this.inEdges;
            }
        } else {
            return null;
        }
    }

    public Iterable<Vertex> getVertices(final Direction direction, final String... labels) {
        return null;
    }

    public void addOutEdge(final FaunusEdge edge) {
        this.outEdges.add(edge);
    }

    public void setOutEdges(final List<Edge> outEdges) {
        this.outEdges = outEdges;
    }


    public void write(final DataOutput out) throws IOException {
        out.writeByte(ElementType.VERTEX.val);
        out.writeLong(this.id);
        EdgeArray.write((List) this.outEdges, out);
        ElementProperties.write(this.properties, out);

    }

    public void readFields(final DataInput in) throws IOException {
        in.readByte();
        this.id = in.readLong();
        this.outEdges = (List) EdgeArray.readFields(in);
        this.properties = ElementProperties.readFields(in);
    }

    public String toString() {
        return StringFactory.vertexString(this);
    }

    public static class Comparator extends WritableComparator {
        public Comparator() {
            super(FaunusVertex.class);
        }

        @Override
        public int compare(final byte[] vertex1, final int start1, final int length1, final byte[] vertex2, final int start2, final int length2) {
            // 0 byte is the element type
            // the next 8 bytes are the long id

            final ByteBuffer buffer1 = ByteBuffer.wrap(vertex1);
            final ByteBuffer buffer2 = ByteBuffer.wrap(vertex2);

            final Byte type1 = buffer1.get();
            final Byte type2 = buffer2.get();
            if (!type1.equals(type2)) {
                return type1.compareTo(type2);
            }
            return (((Long) buffer1.getLong()).compareTo(buffer2.getLong()));
        }

        @Override
        public int compare(final WritableComparable a, final WritableComparable b) {
            if (a instanceof FaunusElement && b instanceof FaunusElement)
                return ((Long) ((FaunusElement) a).getId()).compareTo((Long) ((FaunusElement) b).getId());
            else
                return super.compare(a, b);
        }
    }

}
