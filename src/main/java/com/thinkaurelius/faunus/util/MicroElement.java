package com.thinkaurelius.faunus.util;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class MicroElement {

    protected final long id;

    public MicroElement(final long id) {
        this.id = id;
    }

    public long getId() {
        return this.id;
    }
}
