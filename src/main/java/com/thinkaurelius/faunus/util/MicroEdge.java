package com.thinkaurelius.faunus.util;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class MicroEdge extends MicroElement {

    private static final String E1 = "e[";
    private static final String E2 = "]";

    public MicroEdge(final long id) {
        super(id);
    }

    public String toString() {
        return E1 + this.id + E2;
    }
}