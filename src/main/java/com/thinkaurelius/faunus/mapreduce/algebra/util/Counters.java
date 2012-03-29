package com.thinkaurelius.faunus.mapreduce.algebra.util;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public enum Counters {

    EDGES_ALLOWED_BY_LABEL,
    EDGES_FILTERED_BY_LABEL,
    PROPERTIES_REMOVED,
    EDGES_TRAVERSED,
    EDGES_TRANSPOSED

    /*private String value;

    Counters(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    @Override
    public String toString() {
        return this.getValue();
    }*/

}
