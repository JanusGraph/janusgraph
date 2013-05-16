package com.thinkaurelius.titan.graphdb.query.keycondition;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public interface KeyCondition<K> {

    public enum Type { AND, OR, NOT, LITERAL}

    public Type getType();

    public Iterable<KeyCondition<K>> getChildren();

    public boolean hasChildren();

    public int hashCode();

    public boolean equals(Object other);

    public static final KeyCondition NO_CONDITION = KeyAnd.of();

}
