package com.thinkaurelius.faunus.formats.rexster.util;

import com.tinkerpop.blueprints.Element;

/**
 * All graph element identifiers must be of the long data type.  Implementations of this
 * interface makes it possible to control the conversion of the identifier in the
 * VertexToFaunusBinary utility class.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public interface ElementIdHandler {
    long convertIdentifier(final Element element);
}
