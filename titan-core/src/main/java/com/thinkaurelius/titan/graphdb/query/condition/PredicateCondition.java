package com.thinkaurelius.titan.graphdb.query.condition;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.graphdb.internal.InternalElement;
import com.thinkaurelius.titan.graphdb.query.TitanPredicate;
import com.thinkaurelius.titan.graphdb.query.VertexCentricQueryBuilder;
import com.tinkerpop.blueprints.Direction;
import org.apache.commons.lang.builder.HashCodeBuilder;

import java.util.Iterator;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class PredicateCondition<K, E extends TitanElement> extends Literal<E> {

    private final K key;
    private final TitanPredicate predicate;
    private final Object value;

    public PredicateCondition(K key, TitanPredicate predicate, Object value) {
        Preconditions.checkNotNull(key);
        Preconditions.checkArgument(key instanceof String || key instanceof TitanType);
        Preconditions.checkNotNull(predicate);
        this.key = key;
        this.predicate = predicate;
        this.value = value;
    }


    private boolean satisfiesCondition(Object value) {
        return predicate.evaluate(value, this.value);
    }

    @Override
    public boolean evaluate(E element) {
        TitanType type;
        if (key instanceof String) {
            type = ((InternalElement) element).tx().getType((String) key);
            if (type == null)
                return satisfiesCondition(null);
        } else {
            type = (TitanType) key;
        }

        Preconditions.checkNotNull(type);

        if (type.isPropertyKey()) {
            if (type.isUnique(Direction.OUT))
                return satisfiesCondition(element.getProperty((TitanKey) type));

            Iterator<TitanProperty> iter = ((VertexCentricQueryBuilder) ((TitanVertex) element).query()).type(type).includeHidden().properties().iterator();

            if (iter.hasNext()) {
                while (iter.hasNext()) {
                    if (satisfiesCondition(iter.next().getValue()))
                        return true;
                }

                return false;
            }

            return satisfiesCondition(null);
        } else {
            Preconditions.checkArgument(type.isUnique(Direction.OUT));
            return satisfiesCondition(((TitanRelation) element).getProperty((TitanLabel) type));
        }
    }

    public K getKey() {
        return key;
    }

    public TitanPredicate getPredicate() {
        return predicate;
    }

    public Object getValue() {
        return value;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(getType()).append(key).append(predicate).append(value).toHashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (this == other)
            return true;

        if (other == null || !getClass().isInstance(other))
            return false;

        PredicateCondition oth = (PredicateCondition) other;
        return key.equals(oth.key) && predicate.equals(oth.predicate) && value.equals(oth.value);
    }

    @Override
    public String toString() {
        return key.toString() + " " + predicate.toString() + " " + String.valueOf(value);
    }

    public static <K, E extends TitanElement> PredicateCondition<K, E> of(K key, TitanPredicate titanPredicate, Object condition) {
        return new PredicateCondition<K, E>(key, titanPredicate, condition);
    }

}
