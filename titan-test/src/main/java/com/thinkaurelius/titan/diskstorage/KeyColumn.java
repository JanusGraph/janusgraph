package com.thinkaurelius.titan.diskstorage;

import org.apache.commons.lang.builder.HashCodeBuilder;

public class KeyColumn {

    public final int key;
    public final int column;

    public KeyColumn(int key, int column) {
        this.key = key;
        this.column = column;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(key).append(column).toHashCode();
    }

    public boolean equals(Object other) {
        if (this == other) return true;
        else if (!getClass().isInstance(other)) return false;
        KeyColumn oth = (KeyColumn) other;
        return key == oth.key && column == oth.column;
    }

}
