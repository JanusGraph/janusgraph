package com.thinkaurelius.titan.graphdb.serializer;

import java.io.Serializable;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class SpecialInt implements Serializable {

    private int value;

    public SpecialInt(int value) {
        this.value=value;
    }

    public int getValue() {
        return value;
    }

}
