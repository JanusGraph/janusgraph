package com.thinkaurelius.titan.graphdb.tinkerpop;

import com.thinkaurelius.titan.core.TitanEdge;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.graphdb.relations.RelationIdentifier;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;

/**
 * Created by bryn on 07/05/15.
 */
public class ElementUtils {

    public static long getVertexId(Object id) {
        if (null == id) return 0;

        if (id instanceof TitanVertex) //allows vertices to be "re-attached" to the current transaction
            return ((TitanVertex) id).longId();
        if (id instanceof Long)
            return (Long) id;
        if (id instanceof Number)
            return ((Number) id).longValue();

        try {
            // handles the case of a user passing a "detached" Vertex (DetachedVertex, StarVertex, etc).
            if (id instanceof Vertex)
                return Long.parseLong(((Vertex) id).id().toString());
            else
                return Long.valueOf(id.toString()).longValue();
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    public static RelationIdentifier getEdgeId(Object id) {
        if (null == id) return null;

        try {
            if (id instanceof TitanEdge) return (RelationIdentifier) ((TitanEdge) id).id();
            else if (id instanceof RelationIdentifier) return (RelationIdentifier) id;
            else if (id instanceof String) return RelationIdentifier.parse((String) id);
            else if (id instanceof long[]) return RelationIdentifier.get((long[]) id);
            else if (id instanceof int[]) return RelationIdentifier.get((int[]) id);
        } catch (IllegalArgumentException e) {
            //swallow since null will be returned below
        }
        return null;
    }

    public static void verifyArgsMustBeEitherIdorElement(Object... ids) {
        assert ids.length>0;
        int numElements = 0;
        for (int i = 0; i < ids.length; i++) {
            if (ids[i] instanceof Element) numElements++;
        }
        if (numElements>0 && numElements<ids.length) throw Graph.Exceptions.idArgsMustBeEitherIdOrElement();
    }
}