package com.thinkaurelius.titan.hadoop.formats.rexster.util;

import com.thinkaurelius.titan.core.TitanElement;
import com.tinkerpop.blueprints.Element;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class TitanBerkeleyJEElementIdHandler implements ElementIdHandler {
    @Override
    public long convertIdentifier(final Element element) {
        return ((TitanElement) element).getID();
    }
}