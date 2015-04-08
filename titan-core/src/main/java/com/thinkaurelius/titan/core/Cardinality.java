package com.thinkaurelius.titan.core;

// TODO is this vestigial now that TP3's VertexProperty.Cardinality exists?

import org.apache.tinkerpop.gremlin.structure.VertexProperty;

/**
 * The cardinality of the values associated with given key for a particular element.
 *
* @author Matthias Broecheler (me@matthiasb.com)
*/
public enum Cardinality {

    /**
     * Only a single value may be associated with the given key.
     */
    SINGLE,

    /**
     * Multiple values and duplicate values may be associated with the given key.
     */
    LIST,


    /**
     * Multiple but distinct values may be associated with the given key.
     */
    SET;

    public org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality convert() {
        switch (this) {
            case SINGLE: return VertexProperty.Cardinality.single;
            case LIST: return VertexProperty.Cardinality.list;
            case SET: return VertexProperty.Cardinality.set;
            default: throw new AssertionError("Unrecognized cardinality: " + this);
        }
    }

    public static Cardinality convert(org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality cardinality) {
        switch (cardinality) {
            case single: return SINGLE;
            case list: return LIST;
            case set: return SET;
            default: throw new AssertionError("Unrecognized cardinality: " + cardinality);
        }
    }


}
