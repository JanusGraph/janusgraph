package com.thinkaurelius.titan.hadoop;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.hadoop.mapreduce.util.EmptyConfiguration;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.ExceptionFactory;
import com.tinkerpop.blueprints.util.StringFactory;

import org.apache.hadoop.conf.Configuration;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import static com.tinkerpop.blueprints.Direction.IN;
import static com.tinkerpop.blueprints.Direction.OUT;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class FaunusEdge extends FaunusPathElement implements Edge {

    protected long outVertex;
    protected long inVertex;
    private FaunusType label;

    public FaunusEdge() {
        this(EmptyConfiguration.immutable());
    }

    public FaunusEdge(final Configuration configuration) {
        super(configuration, -1l);
        this.label = FaunusType.LINK;

    }

    public FaunusEdge(final Configuration configuration, final DataInput in) throws IOException {
        super(configuration, -1l);
        this.readFields(in);
    }

    public FaunusEdge(final Configuration configuration, final long outVertex, final long inVertex, final String label) {
        this(configuration, -1l, outVertex, inVertex, label);
    }

    public FaunusEdge(final Configuration configuration, final long id, final long outVertex, final long inVertex, final String label) {
        super(configuration, id);
        this.outVertex = outVertex;
        this.inVertex = inVertex;
        setLabel(label);
    }

    @Override
    void updateSchema(final FaunusSerializer.Schema schema) {
        super.updateSchema(schema);
        schema.add(label);
    }

    @Override
    public Vertex getVertex(final Direction direction) {
        if (OUT.equals(direction)) {
            return new FaunusVertex(this.configuration, this.outVertex);
        } else if (IN.equals(direction)) {
            return new FaunusVertex(this.configuration, this.inVertex);
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

    @Override
    public String getLabel() {
        return label.getName();
    }

    public FaunusType getType() {
        return label;
    }

    final void setLabel(FaunusType label) {
        Preconditions.checkNotNull(label);
        this.label = label;
    }

    final void setLabel(String label) {
        setLabel(FaunusType.DEFAULT_MANAGER.get(label));
    }

    //##################################
    // Serialization Proxy
    //##################################

    public void write(final DataOutput out) throws IOException {
        new FaunusSerializer(this.getConf()).writeEdge(this, out);
    }

    public void readFields(final DataInput in) throws IOException {
        new FaunusSerializer(this.getConf()).readEdge(this, in);

    }

    //##################################
    // General Utility
    //##################################

    @Override
    public String toString() {
        return StringFactory.edgeString(this);
    }


    public static class MicroEdge extends MicroElement {

        private static final String E1 = "e[";
        private static final String E2 = "]";

        public MicroEdge(final long id) {
            super(id);
        }

        public String toString() {
            return E1 + this.id + E2;
        }
    }
}
