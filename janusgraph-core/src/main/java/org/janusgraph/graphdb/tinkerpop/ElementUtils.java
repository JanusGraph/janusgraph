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

package org.janusgraph.graphdb.tinkerpop;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraphEdge;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.graphdb.relations.RelationIdentifier;

/**
 * Created by bryn on 07/05/15.
 */
public class ElementUtils {

    public static long getVertexId(Object id) {
        if (null == id) return 0;

        if (id instanceof JanusGraphVertex) //allows vertices to be "re-attached" to the current transaction
            return ((JanusGraphVertex) id).longId();
        if (id instanceof Long)
            return (Long) id;
        if (id instanceof Number)
            return ((Number) id).longValue();

        try {
            // handles the case of a user passing a "detached" Vertex (DetachedVertex, StarVertex, etc).
            if (id instanceof Vertex)
                return Long.parseLong(((Vertex) id).id().toString());
            else
                return Long.parseLong(id.toString());
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    public static RelationIdentifier getEdgeId(Object id) {
        if (null == id) return null;

        try {
            if (id instanceof JanusGraphEdge) return (RelationIdentifier) ((JanusGraphEdge) id).id();
            else if (id instanceof RelationIdentifier) return (RelationIdentifier) id;
            else if (id instanceof String) return RelationIdentifier.parse((String) id);
            else if (id instanceof long[]) return RelationIdentifier.get((long[]) id);
            else if (id instanceof int[]) return RelationIdentifier.get((int[]) id);
        } catch (IllegalArgumentException e) {
            //swallow since null will be returned below
        }
        return null;
    }

    public static void verifyArgsMustBeEitherIdOrElement(Object... ids) {
        assert ids.length>0;
        int numElements = 0;
        for (Object id : ids) {
            if (id instanceof Element) numElements++;
        }
        if (numElements>0 && numElements<ids.length) throw Graph.Exceptions.idArgsMustBeEitherIdOrElement();
    }
}
