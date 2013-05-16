package com.thinkaurelius.titan.graphdb.serializer;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class TestTransientClass {

    private transient int value = 0;

    public TestTransientClass() {

    }

    public TestTransientClass(int value) {
        this.value=value;
    }

    @Override
    public boolean equals(Object oth) {
        if (this==oth) return true;
        else if (oth==null) return false;
        else if (!getClass().isInstance(oth)) return false;
        return value==((TestTransientClass)oth).value;
    }

}
