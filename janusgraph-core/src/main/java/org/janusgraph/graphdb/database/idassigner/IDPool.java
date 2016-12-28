package com.thinkaurelius.titan.graphdb.database.idassigner;

public interface IDPool {

    public long nextID();

    public void close();

}
