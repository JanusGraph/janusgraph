package org.janusgraph.util.datastructures;

import java.util.AbstractMap;

public class Pair<K,V> extends AbstractMap.SimpleImmutableEntry<K,V> {
    public Pair(K key, V value) {
        super(key, value);
    }
}
