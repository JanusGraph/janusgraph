package com.thinkaurelius.faunus;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.ExceptionFactory;
import com.tinkerpop.blueprints.util.StringFactory;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import static com.tinkerpop.blueprints.Direction.IN;
import static com.tinkerpop.blueprints.Direction.OUT;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class FaunusEdge extends FaunusElement implements Edge {

    private static final String LINK = "_link";

    protected long outVertex;
    protected long inVertex;
    private String label;

    public FaunusEdge() {
        super(-1l);
        this.label = LINK;
    }

    public FaunusEdge(final boolean enablePaths) {
        super(-1l);
        this.label = LINK;
        this.enablePath(enablePaths);
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
        setLabel(label);
    }

    public FaunusEdge reuse(final long id, final long outVertex, final long inVertex, final String label) {
        super.reuse(id);
        this.outVertex = outVertex;
        this.inVertex = inVertex;
        this.setLabel(label);
        return this;
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

    protected final void setLabel(String label) {
        this.label = TYPE_MAP.get(label);
    }

    public void write(final DataOutput out) throws IOException {
        super.write(out);
        WritableUtils.writeVLong(out, this.inVertex);
        WritableUtils.writeVLong(out, this.outVertex);
        //WritableUtils.writeCompressedString(out,this.getLabel());
        out.writeUTF(this.label);
    }

    public void readFields(final DataInput in) throws IOException {
        super.readFields(in);
        this.inVertex = WritableUtils.readVLong(in);
        this.outVertex = WritableUtils.readVLong(in);
        //setLabel(WritableUtils.readCompressedString(in));
        setLabel(in.readUTF());
    }

    public void writeCompressed(final DataOutput out, final Direction idToWrite) throws IOException {
        super.write(out);
        if (idToWrite.equals(Direction.IN))
            WritableUtils.writeVLong(out, this.inVertex);
        else if (idToWrite.equals(Direction.OUT))
            WritableUtils.writeVLong(out, this.outVertex);
        else
            throw ExceptionFactory.bothIsNotSupported();
    }

    public void readFieldsCompressed(final DataInput in, final Direction idToRead) throws IOException {
        super.readFields(in);
        if (idToRead.equals(Direction.IN))
            this.inVertex = WritableUtils.readVLong(in);
        else if (idToRead.equals(Direction.OUT))
            this.outVertex = WritableUtils.readVLong(in);
        else
            throw ExceptionFactory.bothIsNotSupported();
        this.label = null;
    }

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
