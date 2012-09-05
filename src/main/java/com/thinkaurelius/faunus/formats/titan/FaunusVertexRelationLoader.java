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
import com.tinkerpop.blueprints.util.ExceptionFactory;

import java.nio.ByteBuffer;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class FaunusVertexRelationLoader implements VertexRelationLoader {

    private final FaunusVertex vertex;
    private final boolean filterSystemTypes = true;

    private FaunusEdge lastEdge = null;
    private boolean isSystemType = false;

    public FaunusVertexRelationLoader(final ByteBuffer key) {
        this(key.getLong());

    }

    public FaunusVertexRelationLoader(final long id) {
        Preconditions.checkArgument(id > 0);
        vertex = new FaunusVertex(id);
    }

    private Object prepareAttribute(Object attribute) {
        if (!FaunusElement.SUPPORTED_ATTRIBUTE_TYPES.contains(attribute.getClass()))
            attribute = attribute.toString();
        return attribute;
    }

    @Override
    public void loadProperty(final long propertyid, final TitanKey key, final Object attribute) {
        if (key == SystemKey.TypeClass) isSystemType = true;
        if (filterSystemTypes && key instanceof SystemType) return;

        vertex.setProperty(key.getName(), prepareAttribute(attribute));
    }

    @Override
    public void loadEdge(final long edgeid, final TitanLabel label, final Direction dir, final long otherVertexId) {
        if (filterSystemTypes && label instanceof SystemType) return;

        switch (dir) {
            case IN:
                lastEdge = new FaunusEdge(edgeid, otherVertexId, getVertexId(), label.getName());
                vertex.addEdge(dir, lastEdge);
                break;
            case OUT:
                lastEdge = new FaunusEdge(edgeid, getVertexId(), otherVertexId, label.getName());
                vertex.addEdge(dir, lastEdge);
                break;
            default:
                throw ExceptionFactory.bothIsNotSupported();
        }
    }

    @Override
    public void addRelationProperty(final TitanKey key, final Object attribute) {
        if (filterSystemTypes && key instanceof SystemType) return;
        lastEdge.setProperty(key.getName(), prepareAttribute(attribute));
    }

    @Override
    public void addRelationEdge(final TitanLabel label, final long vertexId) {
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
