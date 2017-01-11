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
import org.janusgraph.core.*;
import org.janusgraph.graphdb.internal.InternalElement;
import org.janusgraph.graphdb.internal.InternalRelationType;
import org.janusgraph.graphdb.query.JanusGraphPredicate;
import org.janusgraph.graphdb.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.commons.lang.builder.HashCodeBuilder;

import java.util.Iterator;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class PredicateCondition<K, E extends JanusGraphElement> extends Literal<E> {

    private final K key;
    private final JanusGraphPredicate predicate;
    private final Object value;

    public PredicateCondition(K key, JanusGraphPredicate predicate, Object value) {
        Preconditions.checkNotNull(key);
        Preconditions.checkArgument(key instanceof String || key instanceof RelationType);
        Preconditions.checkNotNull(predicate);
        this.key = key;
        this.predicate = predicate;
        this.value = value;
    }


    private boolean satisfiesCondition(Object value) {
        return predicate.test(value, this.value);
    }

    @Override
    public boolean evaluate(E element) {
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
            Iterator<Object> iter = ElementHelper.getValues(element,(PropertyKey)type).iterator();
            if (iter.hasNext()) {
                while (iter.hasNext()) {
                    if (satisfiesCondition(iter.next()))
                        return true;
                }
                return false;
            }
            return satisfiesCondition(null);
        } else {
            assert ((InternalRelationType)type).multiplicity().isUnique(Direction.OUT);
            return satisfiesCondition((JanusGraphVertex)element.value(type.name()));
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

    public static <K, E extends JanusGraphElement> PredicateCondition<K, E> of(K key, JanusGraphPredicate janusgraphPredicate, Object condition) {
        return new PredicateCondition<K, E>(key, janusgraphPredicate, condition);
    }

}
