package com.thinkaurelius.titan.core.util;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.graphdb.idmanagement.IDManager;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class TitanId {

    /**
     * Converts a user provided long id into a Titan vertex id. The id must be positive and can be at most 61 bits long.
     * This method is useful when providing ids during vertex creation via {@link com.tinkerpop.blueprints.Graph#addVertex(Object)}.
     *
     * @param id long id
     * @return a corresponding Titan vertex id
     */
    public static final long toVertexId(long id) {
        Preconditions.checkArgument(id > 0, "Vertex id must be positive: %s", id);
        Preconditions.checkArgument(IDManager.IDType.Vertex.removePadding(Long.MAX_VALUE) >= id, "Vertex id is too large: %s", id);
        return IDManager.IDType.Vertex.addPadding(id);
    }

    /**
     * Converts a Titan vertex id to the user provided id as the inverse mapping of {@link #toVertexId(long)}.
     *
     * @param id Titan vertex id (must be positive)
     * @return original user provided id
     */
    public static final long fromVertexId(long id) {
        Preconditions.checkArgument(id > 0, "Invalid vertex id provided: %s", id);
        return IDManager.IDType.Vertex.removePadding(id);
    }

    /**
     * Converts a Titan vertex id of a given vertex to the user provided id as the inverse mapping of {@link #toVertexId(long)}.
     *
     * @param v Vertex
     * @return original user provided id
     */
    public static final long fromVertexID(TitanVertex v) {
        Preconditions.checkArgument(v.hasId(), "Invalid vertex provided: %s", v);
        return fromVertexId(v.getID());
    }


}
