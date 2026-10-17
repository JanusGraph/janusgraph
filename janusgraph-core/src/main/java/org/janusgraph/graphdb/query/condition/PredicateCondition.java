// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.graphdb.query.condition;

import com.google.common.base.Preconditions;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.janusgraph.core.JanusGraphElement;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.RelationType;
import org.janusgraph.core.attribute.Cmp;
import org.janusgraph.graphdb.internal.InternalElement;
import org.janusgraph.graphdb.internal.InternalRelationType;
import org.janusgraph.graphdb.internal.OrderList;
import org.janusgraph.graphdb.query.JanusGraphPredicate;
import org.janusgraph.graphdb.util.ElementHelper;

import java.util.Iterator;
import java.util.Objects;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class PredicateCondition<K, E extends JanusGraphElement> extends Literal<E> {

    private final K key;
    private final JanusGraphPredicate predicate;
    private final Object value;
    private final boolean stringsByCodePoint;

    public PredicateCondition(K key, JanusGraphPredicate predicate, Object value) {
        this(key, predicate, value, false);
    }

    /**
     * @param stringsByCodePoint whether a range of {@link Cmp} compares two strings by their code points
     *                           ({@link OrderList#compareByCodePoint}), as a graph-centric query does, in the order of
     *                           the mixed indexes and of the order which JanusGraph gives the query, rather than by
     *                           UTF-16 code unit, as {@link Cmp#test} does and a vertex-centric query, in the order in
     *                           which the storage backend holds the sort keys of a vertex's relations. It is kept only
     *                           for such a range with a string value, so that any other condition equals its twin
     *                           made with the other order, as a query which leaves out a repeated condition expects
     */
    public PredicateCondition(K key, JanusGraphPredicate predicate, Object value, boolean stringsByCodePoint) {
        if (key != null) {
            Preconditions.checkArgument(key instanceof String || key instanceof RelationType);
        }
        this.key = key;
        this.predicate = Preconditions.checkNotNull(predicate);
        this.value = value;
        this.stringsByCodePoint = stringsByCodePoint && isRange(predicate) && value instanceof String;
    }

    private static boolean isRange(JanusGraphPredicate predicate) {
        return predicate == Cmp.LESS_THAN || predicate == Cmp.LESS_THAN_EQUAL || predicate == Cmp.GREATER_THAN
            || predicate == Cmp.GREATER_THAN_EQUAL;
    }


    private boolean satisfiesCondition(Object value) {
        //Set for a range of Cmp with a string value only
        if (stringsByCodePoint && value instanceof String) {
            final int cmp = OrderList.compareByCodePoint((String) value, (String) this.value);
            switch ((Cmp) predicate) {
                case LESS_THAN: return cmp < 0;
                case LESS_THAN_EQUAL: return cmp <= 0;
                case GREATER_THAN: return cmp > 0;
                case GREATER_THAN_EQUAL: return cmp >= 0;
                default: throw new AssertionError("Not a range: " + predicate);
            }
        }
        return predicate.test(value, this.value);
    }

    @Override
    public boolean evaluate(E element) {
        if (key == null) {
            return false;
        }

        RelationType type;
        if (key instanceof String) {
            type = ((InternalElement) element).tx().getRelationType((String) key);
            if (type == null)
                return satisfiesCondition(null);
        } else {
            type = (RelationType) key;
        }

        Preconditions.checkNotNull(type);

        if (type.isPropertyKey()) {
            Iterator<Object> iterator = ElementHelper.getValues(element,(PropertyKey)type).iterator();
            if (iterator.hasNext()) {
                while (iterator.hasNext()) {
                    if (satisfiesCondition(iterator.next()))
                        return true;
                }
                return false;
            }
            return satisfiesCondition(null);
        } else {
            assert ((InternalRelationType)type).multiplicity().isUnique(Direction.OUT);
            return satisfiesCondition(element.value(type.name()));
        }
    }

    public K getKey() {
        return key;
    }

    public JanusGraphPredicate getPredicate() {
        return predicate;
    }

    public Object getValue() {
        return value;
    }

    @Override
    public int hashCode() {
        return Objects.hash(getType(), key, predicate, value, stringsByCodePoint);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other)
            return true;

        if (other == null || !getClass().isInstance(other))
            return false;

        PredicateCondition oth = (PredicateCondition) other;
        return Objects.equals(key, oth.key) && predicate.equals(oth.predicate) && Objects.equals(value, oth.value)
            && stringsByCodePoint == oth.stringsByCodePoint;
    }

    @Override
    public String toString() {
        return key.toString() + " " + predicate.toString() + " " + value;
    }

    public static <K, E extends JanusGraphElement> PredicateCondition<K, E> of(K key, JanusGraphPredicate janusgraphPredicate, Object condition) {
        return new PredicateCondition<>(key, janusgraphPredicate, condition);
    }

}
