package com.thinkaurelius.faunus.formats.rexster.util;

import com.tinkerpop.blueprints.Element;

/**
 * ElementIdHandler implementation for OrientGraph usage.
 * <p/>
 * OrientDB uses a two part identifier where the first part of the identifier is the cluster id and
 * the second part is a record id.  The two are separated by a colon.  The cluster id represents
 * the physical class of the object (ie. vertex or edge).  This part of the identifier can be stripped.
 * <p/>
 * To reconstruct the id (which is not necessary from the point of view of this ElementIdHandler), simply
 * get the class of "vertex" or "edge" from the OrientGraph instance as follows:
 * <p/>
 * <code>
 * int vertexClusterId = g.getRawGraph().getVertexBaseClass().getDefaultClusterId();
 * int edgeClusterId = g.getRawGraph().getEdgeBaseClass().getDefaultClusterId();
 * </code>
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class OrientElementIdHandler implements ElementIdHandler {
    private static final String SEPARATOR = ":";

    @Override
    public long convertIdentifier(final Element element) {
        final String rid = element.getId().toString();
        final int splitPosition = rid.indexOf(SEPARATOR) + 1;

        if (splitPosition > 0)
            return Long.valueOf(rid.substring(splitPosition));
        else
            throw new IllegalArgumentException(String.format(
                    "Identifer [%s] is not in OrientDB format and can't be converted.", rid));
    }
}
