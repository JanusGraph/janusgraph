package com.thinkaurelius.titan.graphdb.serializer;

import java.util.Arrays;

public class TestClass {

    public long a;
    public long b;
    public short[] s;
    public TestEnum e;

    public TestClass() {

    }

    public TestClass(long a, long b, short[] s, TestEnum e) {
        this.a = a;
        this.b = b;
        this.s = s;
        this.e = e;
    }

    public String toString() {
        StringBuilder str = new StringBuilder();
        str.append("TestClass with: a= ").append(a).append(" | b= ").append(b);
        str.append("\n and s= ").append(Arrays.toString(s));
        return str.toString();
    }

    public boolean equals(Object oth) {
        if (this == oth) return true;
        if (!getClass().isInstance(oth)) return false;
        TestClass other = (TestClass) oth;
        return a == other.a && b == other.b && Arrays.equals(s, other.s) && e.equals(other.e);
    }


}