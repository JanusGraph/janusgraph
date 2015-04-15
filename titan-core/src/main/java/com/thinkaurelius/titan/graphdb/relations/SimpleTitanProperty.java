package com.thinkaurelius.titan.graphdb.relations;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.PropertyKey;
import com.thinkaurelius.titan.core.TitanElement;
import com.thinkaurelius.titan.core.TitanProperty;
import com.thinkaurelius.titan.graphdb.internal.InternalRelation;
import com.thinkaurelius.titan.graphdb.internal.InternalRelationType;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.util.NoSuchElementException;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class SimpleTitanProperty<V> implements TitanProperty<V> {

    private final PropertyKey key;
    private final V value;
    private final InternalRelation relation;

    public SimpleTitanProperty(InternalRelation relation, PropertyKey key, V value) {
        this.key = key;
        this.value = value;
        this.relation = relation;
    }

    @Override
    public PropertyKey propertyKey() {
        return key;
    }

    @Override
    public V value() throws NoSuchElementException {
        return value;
    }

    @Override
    public boolean isPresent() {
        return true;
    }

    @Override
    public TitanElement element() {
        return relation;
    }

    @Override
    public void remove() {
        Preconditions.checkArgument(!relation.isRemoved(), "Cannot modified removed relation");
        relation.it().removePropertyDirect(key);
    }

    @Override
    public String toString() {
        return StringFactory.propertyString(this);
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(relation).append(key).append(value).toHashCode();
    }

    @Override
    public boolean equals(Object oth) {
        if (this==oth) return true;
        else if (oth==null || !getClass().isInstance(oth)) return false;
        SimpleTitanProperty other = (SimpleTitanProperty)oth;
        return relation.equals(other.relation) && key.equals(other.key) &&
                value.equals(other.value);
    }

}
