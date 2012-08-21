package com.thinkaurelius.faunus.formats.titan;

import com.google.common.base.Preconditions;
import com.thinkaurelius.faunus.FaunusEdge;
import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.titan.core.TitanKey;
import com.thinkaurelius.titan.core.TitanLabel;
import com.thinkaurelius.titan.graphdb.database.VertexRelationLoader;
import com.tinkerpop.blueprints.Direction;

import java.nio.ByteBuffer;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class FaunusVertexRelationLoader implements VertexRelationLoader {

    private final FaunusVertex vertex;
    
    private FaunusEdge lastEdge=null;

    public FaunusVertexRelationLoader(ByteBuffer key) {
        this(key.getLong());
    }

    public FaunusVertexRelationLoader(final long id) {
        Preconditions.checkArgument(id>0);
        vertex = new FaunusVertex(id);
    }

    @Override
    public void loadProperty(long propertyid, TitanKey key, Object attribute) {
        vertex.setProperty(key.getName(),attribute);
    }

    @Override
    public void loadEdge(long edgeid, TitanLabel label, Direction dir, long otherVertexId) {
        switch(dir) {
            case IN:
                lastEdge = new FaunusEdge(edgeid,otherVertexId,getVertexId(),label.getName());
                break;
            case OUT:
                lastEdge = new FaunusEdge(edgeid,getVertexId(),otherVertexId,label.getName());
                break;
            default: throw new IllegalArgumentException("Unexpected direction: " + dir);
        }
    }

    @Override
    public void addRelationProperty(TitanKey key, Object attribute) {
        lastEdge.setProperty(key.getName(),attribute);
    }

    @Override
    public void addRelationEdge(TitanLabel label, long vertexId) {
        //These are ignored in Faunus
        //TODO: should we add a warning?
    }

    @Override
    public long getVertexId() {
        return vertex.getIdAsLong();
    }

    public FaunusVertex getVertex() {
        return vertex;
    }

}
