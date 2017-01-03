package org.janusgraph.core.util;

import com.google.common.base.Preconditions;
import org.janusgraph.core.JanusVertex;
import org.janusgraph.graphdb.idmanagement.IDManager;

/**
 * Utility methods for handling Janus ids and converting them between indexing and storage backend representations.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class JanusId {

    /**
     * Converts a user provided long id into a Janus vertex id. The id must be positive and can be at most 61 bits long.
     * This method is useful when providing ids during vertex creation via {@link com.tinkerpop.gremlin.structure.Graph#addVertex(Object)}.
     *
     * @param id long id
     * @return a corresponding Janus vertex id
     */
    public static final long toVertexId(long id) {
        Preconditions.checkArgument(id > 0, "Vertex id must be positive: %s", id);
        Preconditions.checkArgument(IDManager.VertexIDType.NormalVertex.removePadding(Long.MAX_VALUE) >= id, "Vertex id is too large: %s", id);
        return IDManager.VertexIDType.NormalVertex.addPadding(id);
    }

    /**
     * Converts a Janus vertex id to the user provided id as the inverse mapping of {@link #toVertexId(long)}.
     *
     * @param id Janus vertex id (must be positive)
     * @return original user provided id
     */
    public static final long fromVertexId(long id) {
        Preconditions.checkArgument(id > 0, "Invalid vertex id provided: %s", id);
        return IDManager.VertexIDType.NormalVertex.removePadding(id);
    }

    /**
     * Converts a Janus vertex id of a given vertex to the user provided id as the inverse mapping of {@link #toVertexId(long)}.
     *
     * @param v Vertex
     * @return original user provided id
     */
    public static final long fromVertexID(JanusVertex v) {
        Preconditions.checkArgument(v.hasId(), "Invalid vertex provided: %s", v);
        return fromVertexId(v.longId());
    }


}
