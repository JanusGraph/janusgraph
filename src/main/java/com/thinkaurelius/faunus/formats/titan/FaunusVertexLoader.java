package com.thinkaurelius.faunus.formats.titan;

import com.google.common.base.Preconditions;
import com.thinkaurelius.faunus.FaunusEdge;
import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.titan.core.TitanType;
import com.thinkaurelius.titan.graphdb.database.idhandling.IDHandler;
import com.thinkaurelius.titan.graphdb.types.system.SystemKey;
import com.thinkaurelius.titan.graphdb.types.system.SystemType;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.util.ExceptionFactory;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * Creates a FaunusVertex given a TitanVertex reference.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 * @author Marko A. Rodriguez (marko@markorodriguez.com)
 */

public class FaunusVertexLoader {

    private final boolean filterSystemTypes = true;
    private final FaunusVertex vertex;

    private boolean isSystemType = false;

    public FaunusVertexLoader(final ByteBuffer key) {
        this(IDHandler.getKeyID(key));

    }

    public FaunusVertexLoader(final long id) {
        Preconditions.checkArgument(id > 0);
        this.vertex = new FaunusVertex(id);
    }

    public FaunusVertex getVertex() {
        if (this.filterSystemTypes && this.isSystemType) return null;
        else return this.vertex;
    }

    public RelationFactory getFactory() {
        return new RelationFactory();
    }

    public class RelationFactory implements com.thinkaurelius.titan.graphdb.database.RelationFactory {

        private final Map<String, Object> properties = new HashMap<String, Object>();

        private Direction direction;
        private TitanType type;
        private long relationID;
        private long otherVertexID;
        private Object value;


        @Override
        public long getVertexID() {
            return vertex.getIdAsLong();
        }

        @Override
        public void setDirection(final Direction direction) {
            this.direction = direction;
        }

        @Override
        public void setType(final TitanType type) {
            if (type == SystemKey.TypeClass) isSystemType = true;
            this.type = type;
        }

        @Override
        public void setRelationID(final long relationID) {
            this.relationID = relationID;
        }

        @Override
        public void setOtherVertexID(final long vertexId) {
            this.otherVertexID = vertexId;
        }

        @Override
        public void setValue(final Object value) {
            this.value = value;
        }

        @Override
        public void addProperty(final TitanType type, final Object value) {
            properties.put(type.getName(), value);
        }

        public final boolean isSystemType() {
            return this.type instanceof SystemType;
        }

        public void build(final boolean loadProperties, final boolean loadInEdges, final boolean loadOutEdges) {
            if (filterSystemTypes && this.isSystemType()) return;

            if (loadProperties && this.type.isPropertyKey()) {
                Preconditions.checkNotNull(value);
                vertex.setProperty(this.type.getName(), this.value);
            } else {
                Preconditions.checkArgument(this.type.isEdgeLabel());
                FaunusEdge edge = null;
                if (loadInEdges & this.direction.equals(Direction.IN))
                    edge = new FaunusEdge(this.relationID, this.otherVertexID, getVertexID(), this.type.getName());
                else if (loadOutEdges & this.direction.equals(Direction.OUT))
                    edge = new FaunusEdge(this.relationID, getVertexID(), this.otherVertexID, this.type.getName());
                else if (this.direction.equals(Direction.BOTH))
                    throw ExceptionFactory.bothIsNotSupported();

                if (null != edge) {
                    // load edge properties
                    for (final Map.Entry<String, Object> entry : this.properties.entrySet()) {
                        if (entry.getValue() != null) {
                            edge.setProperty(entry.getKey(), entry.getValue());
                        }
                    }
                    vertex.addEdge(this.direction, edge);
                }
            }
        }
    }
}
