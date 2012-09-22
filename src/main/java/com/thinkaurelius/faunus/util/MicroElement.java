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

    public int hashCode() {
        return Long.valueOf(this.id).hashCode();
    }

    public boolean equals(final Object object) {
        return (object.getClass().equals(this.getClass()) && this.id == ((MicroElement) object).getId());
    }
}
