// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.core.util;

import com.google.common.base.Preconditions;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.graphdb.idmanagement.IDManager;

/**
 * Utility methods for handling JanusGraph ids and converting them between indexing and storage backend representations.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 * @deprecated This class does not produce valid JanusGraph vertex ids as it does not take into account partitioning
 * bits used in vertex id assignment. Use {@link org.janusgraph.graphdb.idmanagement.IDManager}, which can be obtained
 * through {@link org.janusgraph.graphdb.database.StandardJanusGraph#getIDManager()} and includes methods for converting
 * a user id to ({@link org.janusgraph.graphdb.idmanagement.IDManager#toVertexId(long)}) and from
 * ({@link org.janusgraph.graphdb.idmanagement.IDManager#fromVertexId(long)}) JanusGraph vertex id.
 * <p>
 * <pre>
 * <code>IDManager idManager = ((StandardJanusGraph) graph).getIDManager();</code>
 * </pre>
 */
@Deprecated
public class JanusGraphId {

    /**
     * Converts a user provided long id into a JanusGraph vertex id. The id must be positive and can be at most 61 bits long.
     * This method is useful when providing ids during vertex creation via {@link org.apache.tinkerpop.gremlin.structure.Graph#addVertex(Object...)}.
     *
     * @param id long id
     * @return a corresponding JanusGraph vertex id
     * @deprecated Use {@link org.janusgraph.graphdb.idmanagement.IDManager#toVertexId(long)}.
     */
    public static long toVertexId(long id) {
        Preconditions.checkArgument(id > 0, "Vertex id must be positive: %s", id);
        Preconditions.checkArgument(IDManager.VertexIDType.NormalVertex.removePadding(Long.MAX_VALUE) >= id, "Vertex id is too large: %s", id);
        return IDManager.VertexIDType.NormalVertex.addPadding(id);
    }

    /**
     * Converts a JanusGraph vertex id to the user provided id as the inverse mapping of {@link #toVertexId(long)}.
     *
     * @param id JanusGraph vertex id (must be positive)
     * @return original user provided id
     * @deprecated Use {@link org.janusgraph.graphdb.idmanagement.IDManager#fromVertexId(long)}
     */
    public static long fromVertexId(long id) {
        Preconditions.checkArgument(id > 0, "Invalid vertex id provided: %s", id);
        return IDManager.VertexIDType.NormalVertex.removePadding(id);
    }

    /**
     * Converts a JanusGraph vertex id of a given vertex to the user provided id as the inverse mapping of {@link #toVertexId(long)}.
     *
     * @param v Vertex
     * @return original user provided id
     * @deprecated Use {@link org.janusgraph.graphdb.idmanagement.IDManager#fromVertexId(long)}
     */
    public static long fromVertexID(JanusGraphVertex v) {
        Preconditions.checkArgument(v.hasId(), "Invalid vertex provided: %s", v);
        return fromVertexId(v.longId());
    }
}
