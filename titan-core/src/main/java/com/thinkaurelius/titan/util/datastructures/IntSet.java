package com.thinkaurelius.titan.util.datastructures;


public interface IntSet {

    public boolean add(int value);

    public boolean addAll(int[] values);

    public boolean contains(int value);

    public boolean remove(int value);

    public int[] getAll();

    public int size();

}
