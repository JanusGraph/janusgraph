package com.thinkaurelius.faunus.io.graph.util;

import com.thinkaurelius.faunus.io.graph.FaunusEdge;
import com.thinkaurelius.faunus.io.graph.FaunusElement;
import com.thinkaurelius.faunus.io.graph.FaunusVertex;
import org.apache.hadoop.io.GenericWritable;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ElementHolder<T extends FaunusElement> extends GenericWritable {

    private static Class[] CLASSES = {
            FaunusVertex.class,
            FaunusEdge.class
    };

    protected Class[] getTypes() {
        return CLASSES;

    }

    public ElementHolder() {
        super();
    }

    public ElementHolder(final T element) {
        this.set(element);
    }
}
