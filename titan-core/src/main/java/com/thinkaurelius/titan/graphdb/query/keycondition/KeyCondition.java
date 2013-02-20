package com.thinkaurelius.titan.graphdb.query.keycondition;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public interface KeyCondition<K> {

    public enum Type { AND, OR, NOT, LITERAL }

    public Type getType();

    public Iterable<KeyCondition<K>> getChildren();

    public int hashCode();

    public boolean equals(Object other);

}
