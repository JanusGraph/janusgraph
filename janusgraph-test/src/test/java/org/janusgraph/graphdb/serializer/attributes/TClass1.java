package com.thinkaurelius.titan.graphdb.serializer.attributes;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class TClass1 {

    private final long a;
    private final float f;

    public TClass1(long a, float f) {
        this.a = a;
        this.f = f;
    }

    public long getA() {
        return a;
    }

    public float getF() {
        return f;
    }

    @Override
    public boolean equals(Object oth) {
        TClass1 t = (TClass1)oth;
        return a==t.a && f==t.f;
    }
}
