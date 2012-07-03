package com.thinkaurelius.faunus.io.graph;

import com.thinkaurelius.faunus.io.graph.util.ElementProperties;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.ExceptionFactory;
import com.tinkerpop.blueprints.util.StringFactory;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class FaunusEdge extends FaunusElement<Edge> implements Edge, Writable {

    private FaunusVertex outVertex;
    private FaunusVertex inVertex;
    private String label = "_default";

    static {
        WritableComparator.define(FaunusEdge.class, new Comparator());
    }


    public FaunusEdge() {
        super(-1l);
    }

    public FaunusEdge(final DataInput in) throws IOException {
        super(-1l);
        this.readFields(in);
    }

    public FaunusEdge(final FaunusVertex outVertex, final FaunusVertex inVertex, final String label) {
        super(-1l);
        this.outVertex = outVertex;
        this.inVertex = inVertex;
        this.label = label;
    }

    public Vertex getVertex(final Direction direction) {
        if (Direction.OUT.equals(direction)) {
            return outVertex;
        } else if (Direction.IN.equals(direction)) {
            return inVertex;
        } else {
            throw ExceptionFactory.bothIsNotSupported();
        }
    }

    public String getLabel() {
        return label;
    }

    public void write(final DataOutput out) throws IOException {
        out.writeByte(ElementType.EDGE.val);
        out.writeLong(this.id);
        out.writeLong((Long) this.getVertex(Direction.IN).getId());
        out.writeLong((Long) this.getVertex(Direction.OUT).getId());
        out.writeUTF(this.getLabel());
        ElementProperties.write(this.properties, out);
    }

    public void readFields(final DataInput in) throws IOException {
        in.readByte();
        this.id = in.readLong();
        this.inVertex = new FaunusVertex(in.readLong());
        this.outVertex = new FaunusVertex(in.readLong());
        this.label = in.readUTF();
        this.properties = ElementProperties.readFields(in);
    }

    public String toString() {
        return StringFactory.edgeString(this);
    }

    public static class Comparator extends WritableComparator {
        public Comparator() {
            super(FaunusEdge.class);
        }

        @Override
        public int compare(final byte[] edge1, final int start1, final int length1, final byte[] edge2, final int start2, final int length2) {
            // 0 byte is the element type
            // the next 8 bytes are the edge id
            // the next 8 bytes are the in vertex id
            // the next 8 bytes are the out vertex id
            final ByteBuffer buffer1 = ByteBuffer.wrap(edge1);
            final ByteBuffer buffer2 = ByteBuffer.wrap(edge2);

            final Byte type1 = buffer1.get();
            final Byte type2 = buffer2.get();
            if (!type1.equals(type2)) {
                return type1.compareTo(type2);
            }
            buffer1.getLong(); // ignore ids
            buffer2.getLong(); // ignore ids

            Long temp1 = buffer1.getLong();
            Long temp2 = buffer2.getLong();

            if (!temp1.equals(temp2))
                return temp1.compareTo(temp2);

            temp1 = buffer1.getLong();
            temp2 = buffer2.getLong();

            return temp1.compareTo(temp2);
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
