package com.thinkaurelius.faunus.util;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class MicroVertex extends MicroElement {

    private static final String V1 = "v[";
    private static final String V2 = "]";

    public MicroVertex(final long id) {
        super(id);
    }

    public String toString() {
        return V1 + this.id + V2;
    }
}
