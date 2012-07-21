package com.thinkaurelius.faunus;

import com.thinkaurelius.faunus.util.ElementProperties;
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

import static com.tinkerpop.blueprints.Direction.IN;
import static com.tinkerpop.blueprints.Direction.OUT;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class FaunusEdge extends FaunusElement<Edge> implements Edge {

    private static final String DEFAULT = "_default";


    private FaunusVertex outVertex;
    private FaunusVertex inVertex;
    private String label = DEFAULT;

    public FaunusEdge() {
        super(-1l);
    }

    public FaunusEdge(final DataInput in) throws IOException {
        super(-1l);
        this.readFields(in);
    }

    public FaunusEdge(final FaunusVertex outVertex, final FaunusVertex inVertex, final String label) {
        this(-1l, outVertex, inVertex, label);
    }

    public FaunusEdge(final Long id, final FaunusVertex outVertex, final FaunusVertex inVertex, final String label) {
        super(id);
        this.outVertex = outVertex;
        this.inVertex = inVertex;
        this.label = label;
    }

    public Vertex getVertex(final Direction direction) {
        if (OUT.equals(direction)) {
            return outVertex;
        } else if (IN.equals(direction)) {
            return inVertex;
        } else {
            throw ExceptionFactory.bothIsNotSupported();
        }
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(final String label) {
        this.label = label;
    }

    public void write(final DataOutput out) throws IOException {
        out.writeLong(this.id);
        out.writeLong((Long) this.getVertex(IN).getId());
        out.writeLong((Long) this.getVertex(OUT).getId());
        out.writeUTF(this.getLabel());
        ElementProperties.write(this.properties, out);
    }

    public void readFields(final DataInput in) throws IOException {
        this.id = in.readLong();
        this.inVertex = new FaunusVertex(in.readLong());
        this.outVertex = new FaunusVertex(in.readLong());
        this.label = in.readUTF();
        this.properties = ElementProperties.readFields(in);
    }

    public String toString() {
        return StringFactory.edgeString(this);
    }
}
