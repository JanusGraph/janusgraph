package com.thinkaurelius.titan.hadoop;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.EdgeLabel;
import com.thinkaurelius.titan.core.TitanEdge;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.diskstorage.configuration.Configuration;
import com.thinkaurelius.titan.graphdb.relations.EdgeDirection;
import com.thinkaurelius.titan.hadoop.config.ModifiableHadoopConfiguration;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.util.ExceptionFactory;
import com.tinkerpop.blueprints.util.StringFactory;

import org.apache.commons.lang.builder.HashCodeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import static com.tinkerpop.blueprints.Direction.IN;
import static com.tinkerpop.blueprints.Direction.OUT;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class StandardFaunusEdge extends StandardFaunusRelation implements FaunusEdge {

    protected long outVertex;
    protected long inVertex;

    private static final Logger log =
            LoggerFactory.getLogger(StandardFaunusEdge.class);

    public StandardFaunusEdge() {
        this(ModifiableHadoopConfiguration.immutableWithResources());
    }

    public StandardFaunusEdge(final Configuration configuration) {
        super(configuration, FaunusElement.NO_ID, FaunusEdgeLabel.LINK);
    }

    public StandardFaunusEdge(final Configuration configuration, final long outVertex, final long inVertex, final String label) {
        this(configuration, FaunusElement.NO_ID, outVertex, inVertex, label);
    }

    public StandardFaunusEdge(final Configuration configuration, final long id, final long outVertex, final long inVertex, String label) {
        this(configuration,id,outVertex,inVertex, FaunusSchemaManager.getTypeManager(configuration).getOrCreateEdgeLabel(label));
    }

    public StandardFaunusEdge(final Configuration configuration, final long outVertex, final long inVertex, FaunusEdgeLabel label) {
        this(configuration, FaunusElement.NO_ID,outVertex,inVertex,label);
    }

    public StandardFaunusEdge(final Configuration configuration, final long id, final long outVertex, final long inVertex, FaunusEdgeLabel label) {
        super(configuration, id, label);
        this.outVertex = outVertex;
        this.inVertex = inVertex;
    }

    public StandardFaunusEdge(final Configuration configuration, final DataInput in) throws IOException {
        super(configuration, FaunusElement.NO_ID, FaunusEdgeLabel.LINK);
        this.readFields(in);
    }

    @Override
    public EdgeLabel getEdgeLabel() {
        return (EdgeLabel)getType();
    }


    @Override
    public TitanVertex getVertex(int pos) {
        if (pos==0) {
            return new FaunusVertex(configuration, outVertex);
        } else if (pos==1) {
            return new FaunusVertex(configuration, inVertex);
        } else {
            throw ExceptionFactory.bothIsNotSupported();
        }
    }

    @Override
    public TitanVertex getVertex(final Direction direction) {
        return getVertex(EdgeDirection.position(direction));
    }

    @Override
    public String getLabel() {
        return getTypeName();
    }

    @Override
    public TitanVertex getOtherVertex(TitanVertex vertex) {
        for (int i=0;i<2;i++) {
            if (getVertex(i).equals(vertex)) return getVertex((i+1)%2);
        }
        throw new IllegalArgumentException("Edge is not incident on vertex: "+ vertex);
    }

    @Override
    public boolean isDirected() {
        return getEdgeLabel().isDirected();
    }

    @Override
    public boolean isUnidirected() {
        return getEdgeLabel().isUnidirected();
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

    final void setLabel(FaunusEdgeLabel label) {
        Preconditions.checkNotNull(label);
        setType(label);
    }

    //##################################
    // Serialization Proxy
    //##################################

    @Override
    public void write(final DataOutput out) throws IOException {
        new FaunusSerializer(this.configuration).writeEdge(this, out);
        //log.debug("Serialized   {} with {} paths", this, tracker.paths.size());
    }

    @Override
    public void readFields(final DataInput in) throws IOException {
        new FaunusSerializer(this.configuration).readEdge(this, in);
        //log.debug("Deserialized {} with paths {}", this, tracker.paths);
    }

    //##################################
    // General Utility
    //##################################

    @Override
    public String toString() {
        try {
            return StringFactory.edgeString(this);
        } catch (NullPointerException e) {
            return "StandardFaunusEdge[null]";
        }
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(outVertex).append(inVertex).append(getLongId()).append(getType()).toHashCode();
    }

    @Override
    public boolean equals(Object oth) {
        if (this == oth) return true;
        else if (oth == null || !(oth instanceof TitanEdge)) return false;
        TitanEdge e = (TitanEdge) oth;
        if (hasId() || e.hasId()) return getLongId()==e.getLongId();
        return getType().equals(e.getEdgeLabel()) && outVertex==e.getVertex(Direction.OUT).getLongId() &&
                inVertex==e.getVertex(Direction.IN).getLongId();
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
