package com.thinkaurelius.faunus.formats.json;

import com.thinkaurelius.faunus.FaunusEdge;
import com.thinkaurelius.faunus.FaunusVertex;
import com.tinkerpop.blueprints.util.io.graphson.ElementFactory;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class FaunusElementFactory implements ElementFactory<FaunusVertex, FaunusEdge> {
    @Override
    public FaunusEdge createEdge(Object id, FaunusVertex out, FaunusVertex in, String label) {
        if (!(out instanceof FaunusVertex) || !(in instanceof FaunusVertex)) {
            throw new IllegalArgumentException("Both in and out vertices must be of type Faunus Vertex");
        }

        return new FaunusEdge(convertIdentifier(id), (FaunusVertex) out, (FaunusVertex) in, label);
    }

    @Override
    public FaunusVertex createVertex(Object id) {
        return new FaunusVertex(convertIdentifier(id));
    }

    private long convertIdentifier(Object id) {
        long identifier = -1l;
        if (id != null) {
            try {
                identifier = Long.parseLong(id.toString());
            } catch (NumberFormatException nfe) {
                identifier = -1l;
            }
        }
        return identifier;
    }
}
