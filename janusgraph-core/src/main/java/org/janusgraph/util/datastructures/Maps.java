package com.thinkaurelius.titan.util.datastructures;

import java.util.concurrent.ConcurrentMap;

/**
 * Utility class for Maps
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class Maps {

    public final static <K, V> V putIfAbsent(ConcurrentMap<K, V> map, K key, Factory<V> factory) {
        V res = map.get(key);
        if (res != null) return res;
        else {
            V newobj = factory.create();
            res = map.putIfAbsent(key, newobj);
            if (res == null) return newobj;
            else return res;
        }
    }


}
