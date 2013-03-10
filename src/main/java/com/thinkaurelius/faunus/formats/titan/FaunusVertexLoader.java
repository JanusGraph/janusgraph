package com.thinkaurelius.faunus.formats.titan;

import com.google.common.base.Preconditions;
import com.thinkaurelius.faunus.FaunusEdge;
import com.thinkaurelius.faunus.FaunusElement;
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
 * (c) Matthias Broecheler (me@matthiasb.com)
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
        vertex = new FaunusVertex(id);
    }

    public FaunusVertex getVertex() {
        if (filterSystemTypes && isSystemType) return null;
        else return vertex;
    }

    public RelationFactory getFactory() {
        return new RelationFactory();
    }

    private static Object prepareAttribute(Object attribute) {
        if (!FaunusElement.SUPPORTED_ATTRIBUTE_TYPES.contains(attribute.getClass()))
            attribute = attribute.toString();
        return attribute;
    }

    public class RelationFactory implements com.thinkaurelius.titan.graphdb.database.RelationFactory {

        private final Map<String,Object> properties = new HashMap<String,Object>();

        private Direction dir;
        private TitanType type;
        private long relationID;
        private long otherVertexID;
        private Object value;


        @Override
        public long getVertexID() {
            return vertex.getIdAsLong();
        }

        @Override
        public void setDirection(Direction dir) {
            this.dir=dir;
        }

        @Override
        public void setType(TitanType type) {
            if (type == SystemKey.TypeClass) isSystemType = true;
            this.type = type;
        }

        @Override
        public void setRelationID(long relationID) {
            this.relationID = relationID;
        }

        @Override
        public void setOtherVertexID(long vertexId) {
            this.otherVertexID=vertexId;
        }

        @Override
        public void setValue(Object value) {
            this.value=value;
        }

        @Override
        public void addProperty(TitanType type, Object value) {
            properties.put(type.getName(),value);
        }

        public void build() {
            if (filterSystemTypes && type instanceof SystemType) return;

            if (type.isPropertyKey()) {
                Preconditions.checkNotNull(value);
                vertex.setProperty(type.getName(), prepareAttribute(value));
            } else {
                Preconditions.checkArgument(type.isEdgeLabel());
                FaunusEdge edge = null;
                switch (dir) {
                    case IN:
                        edge = new FaunusEdge(relationID, otherVertexID, getVertexID(), type.getName());
                        break;
                    case OUT:
                        edge = new FaunusEdge(relationID, getVertexID(), otherVertexID, type.getName());
                        break;
                    default:
                        throw ExceptionFactory.bothIsNotSupported();
                }
                //Add properties
                for (Map.Entry<String,Object> entry : properties.entrySet()) {
                    if (entry.getValue()!=null) {
                        edge.setProperty(entry.getKey(),entry.getValue());
                    }
                }
                vertex.addEdge(dir, edge);
            }
        }
    }
}
