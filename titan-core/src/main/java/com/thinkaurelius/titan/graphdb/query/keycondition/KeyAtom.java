package com.thinkaurelius.titan.graphdb.query.keycondition;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang.builder.HashCodeBuilder;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class KeyAtom<K> implements KeyCondition<K> {

    private final K key;
    private final Relation relation;
    private final Object condition;

    public KeyAtom(K key, Relation relation, Object condition) {
        Preconditions.checkNotNull(key);
        Preconditions.checkNotNull(relation);
        this.key = key;
        this.relation = relation;
        this.condition = condition;
    }

    @Override
    public Type getType() {
        return Type.LITERAL;
    }

    @Override
    public boolean hasChildren() {
        return false;
    }

    @Override
    public Iterable<KeyCondition<K>> getChildren() {
        return ImmutableList.of();
    }

    public K getKey() {
        return key;
    }

    public Relation getRelation() {
        return relation;
    }

    public Object getCondition() {
        return condition;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(getType()).append(key).append(relation).append(condition).toHashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (this==other) return true;
        else if (other==null) return false;
        else if (!getClass().isInstance(other)) return false;
        KeyAtom oth = (KeyAtom)other;
        return key.equals(oth.key) && relation.equals(oth.relation) && condition.equals(oth.condition);
    }

    @Override
    public String toString() {
        return key.toString()+relation.toString()+String.valueOf(condition);
    }

    public static final<K> KeyAtom<K> of(K key, Relation relation, Object condition) {
        return new KeyAtom<K>(key, relation, condition);
    }

}
