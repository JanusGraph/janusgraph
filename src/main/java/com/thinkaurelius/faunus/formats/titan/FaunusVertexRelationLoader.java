package com.thinkaurelius.faunus.formats.titan;

import com.google.common.base.Preconditions;
import com.thinkaurelius.faunus.FaunusEdge;
import com.thinkaurelius.faunus.FaunusElement;
import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.titan.core.TitanKey;
import com.thinkaurelius.titan.core.TitanLabel;
import com.thinkaurelius.titan.graphdb.database.VertexRelationLoader;
import com.thinkaurelius.titan.graphdb.types.system.SystemKey;
import com.thinkaurelius.titan.graphdb.types.system.SystemType;
import com.tinkerpop.blueprints.Direction;

import java.nio.ByteBuffer;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class FaunusVertexRelationLoader implements VertexRelationLoader {

    private final FaunusVertex vertex;
    private final boolean filterSystemTypes = true; 
    
    private FaunusEdge lastEdge=null;
    private boolean isSystemType=false;

    public FaunusVertexRelationLoader(ByteBuffer key) {
        this(key.getLong());

    }

    public FaunusVertexRelationLoader(final long id) {
        Preconditions.checkArgument(id>0);
        vertex = new FaunusVertex(id);
    }

    private Object prepareAttribute(Object attribute) {
        if (!FaunusElement.SUPPORTED_ATTRIBUTE_TYPES.contains(attribute.getClass()))
            attribute = attribute.toString();
        return attribute;
    }
    
    @Override
    public void loadProperty(long propertyid, TitanKey key, Object attribute) {
        if (key == SystemKey.TypeClass) isSystemType=true;
        if (filterSystemTypes && key instanceof SystemType) return;

        vertex.setProperty(key.getName(),prepareAttribute(attribute));
    }

    @Override
    public void loadEdge(long edgeid, TitanLabel label, Direction dir, long otherVertexId) {
        if (filterSystemTypes && label instanceof SystemType) return;

        switch(dir) {
            case IN:
                lastEdge = new FaunusEdge(edgeid,otherVertexId,getVertexId(),label.getName());
                vertex.addEdge(dir,lastEdge);
                break;
            case OUT:
                lastEdge = new FaunusEdge(edgeid,getVertexId(),otherVertexId,label.getName());
                vertex.addEdge(dir,lastEdge);
                break;
            default: throw new IllegalArgumentException("Unexpected direction: " + dir);
        }
    }

    @Override
    public void addRelationProperty(TitanKey key, Object attribute) {
        if (filterSystemTypes && key instanceof SystemType) return;
        lastEdge.setProperty(key.getName(),prepareAttribute(attribute));
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
        if (filterSystemTypes && isSystemType) return null;
        else return vertex;
    }

}
