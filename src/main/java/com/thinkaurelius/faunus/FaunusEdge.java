package com.thinkaurelius.faunus;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.ExceptionFactory;
import com.tinkerpop.blueprints.util.StringFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import static com.tinkerpop.blueprints.Direction.IN;
import static com.tinkerpop.blueprints.Direction.OUT;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class FaunusEdge extends FaunusElement implements Edge {

    private static final String DEFAULT = "_default";

    private long outVertex;
    private long inVertex;
    private String label;

    public FaunusEdge() {
        super(-1l);
        this.label = DEFAULT;
    }

    public FaunusEdge(final DataInput in) throws IOException {
        super(-1l);
        this.readFields(in);
    }

    public FaunusEdge(final long outVertex, final long inVertex, final String label) {
        this(-1l, outVertex, inVertex, label);
    }

    public FaunusEdge(final long id, final long outVertex, final long inVertex, final String label) {
        super(id);
        this.outVertex = outVertex;
        this.inVertex = inVertex;
        this.label = label;
    }

    public Vertex getVertex(final Direction direction) {
        if (OUT.equals(direction)) {
            return new FaunusVertex(this.outVertex);
        } else if (IN.equals(direction)) {
            return new FaunusVertex(this.inVertex);
        } else {
            throw ExceptionFactory.bothIsNotSupported();
        }
    }

    public long getVertexId(final Direction direction) {
        if (OUT.equals(direction)) {
            return this.outVertex;
        } else if (IN.equals(direction)) {
            return this.inVertex;
        } else {
            throw ExceptionFactory.bothIsNotSupported();
        }
    }

    public String getLabel() {
        return this.label;
    }

    public void write(final DataOutput out) throws IOException {
        out.writeLong(this.id);
        out.writeLong(this.inVertex);
        out.writeLong(this.outVertex);
        out.writeUTF(this.getLabel());
        ElementProperties.write(this.properties, out);
    }

    public void readFields(final DataInput in) throws IOException {
        this.id = in.readLong();
        this.inVertex = in.readLong();
        this.outVertex = in.readLong();
        this.label = in.readUTF();
        this.properties = ElementProperties.readFields(in);
    }

    public String toString() {
        return StringFactory.edgeString(this);
    }
}
