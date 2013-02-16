package com.thinkaurelius.titan.diskstorage;

import com.thinkaurelius.titan.diskstorage.keycolumnvalue.Entry;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class Mutation<E,K> {

    private List<E> additions;

    private List<K> deletions;

    public Mutation(List<E> additions, List<K> deletions) {
        this.additions = additions;
        this.deletions = deletions;
    }

    public Mutation() {
        this.additions = null;
        this.deletions = null;
    }

    public boolean hasAdditions() {
        return additions != null && !additions.isEmpty();
    }

    public boolean hasDeletions() {
        return deletions != null && !deletions.isEmpty();
    }

    public List<E> getAdditions() {
        return additions;
    }

    public List<K> getDeletions() {
        return deletions;
    }

    public void addition(E entry) {
        if (additions==null) additions = new ArrayList<E>();
        additions.add(entry);
    }

    public void deletion(K key) {
        if (deletions==null) deletions = new ArrayList<K>();
        deletions.add(key);
    }

    public void merge(Mutation<E,K> m) {

        if (null == m) {
            return;
        }

        if (null != m.additions) {
            if (null == additions) additions = m.additions;
            else additions.addAll(m.additions);
        }

        if (null != m.deletions) {
            if (null == deletions) deletions = m.deletions;
            else deletions.addAll(m.deletions);
        }
    }

}
