package com.thinkaurelius.faunus;

import com.thinkaurelius.faunus.util.EdgeArray;
import com.thinkaurelius.faunus.util.ElementProperties;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Query;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.ExceptionFactory;
import com.tinkerpop.blueprints.util.MultiIterable;
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

import static com.tinkerpop.blueprints.Direction.IN;
import static com.tinkerpop.blueprints.Direction.OUT;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class FaunusVertex extends FaunusElement implements Vertex, WritableComparable<FaunusVertex> {

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
        throw new UnsupportedOperationException();
    }

    public Iterable<Edge> getEdges(final Direction direction, final String... labels) {
        final Set<String> legalLabels;
        if (null != labels && labels.length > 0)
            legalLabels = new HashSet<String>(Arrays.asList(labels));
        else
            legalLabels = null;

        if (OUT.equals(direction)) {
            if (null != legalLabels) {
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
        } else if (IN.equals(direction)) {
            if (null != legalLabels) {
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
            return new MultiIterable<Edge>(Arrays.asList(this.getEdges(IN, labels), this.getEdges(OUT, labels)));
        }
    }

    public Iterable<Vertex> getVertices(final Direction direction, final String... labels) {
        return null;
    }

    public FaunusVertex addEdge(final Direction direction, final FaunusEdge edge) {
        if (OUT.equals(direction))
            this.outEdges.add(edge);
        else if (IN.equals(direction))
            this.inEdges.add(edge);
        else
            throw ExceptionFactory.bothIsNotSupported();

        return this;
    }

    public void setEdges(final Direction direction, final List<Edge> edges) {
        if (OUT.equals(direction))
            this.outEdges = edges;
        else if (IN.equals(direction))
            this.inEdges = edges;
        else
            throw ExceptionFactory.bothIsNotSupported();
    }

    public void write(final DataOutput out) throws IOException {
        out.writeLong(this.id);
        EdgeArray.write((List) this.inEdges, out);
        EdgeArray.write((List) this.outEdges, out);
        ElementProperties.write(this.properties, out);

    }

    public void readFields(final DataInput in) throws IOException {
        this.id = in.readLong();
        this.inEdges = (List) EdgeArray.readFields(in);
        this.outEdges = (List) EdgeArray.readFields(in);
        this.properties = ElementProperties.readFields(in);
    }

    public int compareTo(final FaunusVertex other) {
        return new Long(this.id).compareTo((Long) other.getId());
    }

    public String toString() {
        return StringFactory.vertexString(this);
    }

    public FaunusVertex cloneIdAndProperties() {
        final FaunusVertex clone = new FaunusVertex(this.getIdAsLong());
        clone.setProperties(this.getProperties());
        return clone;
    }

    public static class Comparator extends WritableComparator {
        public Comparator() {
            super(FaunusVertex.class);
        }

        @Override
        public int compare(final byte[] vertex1, final int start1, final int length1, final byte[] vertex2, final int start2, final int length2) {
            // the first 8 bytes are the long id
            final ByteBuffer buffer1 = ByteBuffer.wrap(vertex1);
            final ByteBuffer buffer2 = ByteBuffer.wrap(vertex2);
            return (((Long) buffer1.getLong()).compareTo(buffer2.getLong()));
        }

        @Override
        public int compare(final WritableComparable a, final WritableComparable b) {
            if (a instanceof FaunusVertex && b instanceof FaunusVertex)
                return (((FaunusVertex) a).getIdAsLong()).compareTo(((FaunusVertex) b).getIdAsLong());
            else
                return super.compare(a, b);
        }
    }
}
