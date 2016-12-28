package com.thinkaurelius.titan.diskstorage.util;

/**
* @author Matthias Broecheler (me@matthiasb.com)
*/
public enum CacheMetricsAction {

    RETRIEVAL("retrievals"), MISS("misses"), EXPIRE("expire");

    private final String name;

    CacheMetricsAction(String name) {
        this.name = name;
    }

    public String getName() { return name; }
}
