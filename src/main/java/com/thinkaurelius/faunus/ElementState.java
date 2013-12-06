package com.thinkaurelius.faunus;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public enum ElementState {

    NEW, //The FaunusElement is new with respect to its base source
    DELETED, //The FaunusElement has been deleted compared to its base source
    LOADED; //The FaunusElement has been loaded from a base source

    public byte getByteValue() {
        return (byte)this.ordinal();
    }

    public static ElementState valueOf(byte ordinal) {
        for (ElementState s : ElementState.values()) if (s.ordinal()==ordinal) return s;
        throw new IllegalArgumentException("Unknown state id: " + ordinal);
    }

}
