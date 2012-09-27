package com.thinkaurelius.faunus.formats.rexster.util;

import com.tinkerpop.blueprints.Element;

/**
 * Assumes that the identifier is a long itself or some other representation of a long.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class DefaultElementIdHandler implements ElementIdHandler {
    @Override
    public long convertIdentifier(final Element element) {
        final Object id = element.getId();
        if (id instanceof Long)
            return (Long) id;
        else if (id instanceof Number)
            return ((Number) id).longValue();
        else
            return Long.valueOf(id.toString());
    }
}
