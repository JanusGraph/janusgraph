package com.thinkaurelius.titan.hadoop;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public enum ElementState {

    NEW, //The HadoopElement is new with respect to its base source
    DELETED, //The HadoopElement has been deleted compared to its base source
    LOADED; //The HadoopElement has been loaded from a base source

    public byte getByteValue() {
        return (byte) this.ordinal();
    }

    public static ElementState valueOf(byte ordinal) {
        for (ElementState s : ElementState.values()) if (s.ordinal() == ordinal) return s;
        throw new IllegalArgumentException("Unknown state id: " + ordinal);
    }

}
