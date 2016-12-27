package com.thinkaurelius.titan.graphdb.serializer.attributes;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class TClass2 {

    private final String s;
    private final int i;

    public TClass2(String s, int i) {
        this.s = s;
        this.i = i;
    }

    public String getS() {
        return s;
    }

    public int getI() {
        return i;
    }

    @Override
    public boolean equals(Object oth) {
        TClass2 t = (TClass2)oth;
        return s.equals(t.s) && i==t.i;
    }
}
