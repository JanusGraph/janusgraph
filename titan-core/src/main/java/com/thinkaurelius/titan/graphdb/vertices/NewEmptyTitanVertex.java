package com.thinkaurelius.titan.graphdb.vertices;


public abstract class NewEmptyTitanVertex extends LoadedEmptyTitanVertex {


    @Override
    public boolean isLoaded() {
        return false;
    }

    @Override
    public boolean isNew() {
        return true;
    }

}
