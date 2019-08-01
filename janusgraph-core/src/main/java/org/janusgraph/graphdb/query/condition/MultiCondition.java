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

import org.janusgraph.core.JanusGraphElement;

import java.util.ArrayList;
import java.util.Objects;

/**
 * Abstract condition element that combines multiple conditions (for instance, AND, OR).
 *
 * @see And
 * @see Or
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public abstract class MultiCondition<E extends JanusGraphElement> extends ArrayList<Condition<E>> implements Condition<E> {

    MultiCondition() {
        this(5);
    }

    MultiCondition(int capacity) {
        super(capacity);
    }

    MultiCondition(final Condition<E>... conditions) {
        super(conditions.length);
        for (Condition<E> condition : conditions) {
            assert condition != null;
            super.add(condition);
        }
    }

    MultiCondition(MultiCondition<E> cond) {
        this(cond.size());
        super.addAll(cond);
    }

    public boolean add(Condition<E> condition) {
        assert condition != null;
        return super.add(condition);
    }

    @Override
    public boolean hasChildren() {
        return !super.isEmpty();
    }

    @Override
    public int numChildren() {
        return super.size();
    }

    @Override
    public Iterable<Condition<E>> getChildren() {
        return this;
    }

    @Override
    public int hashCode() {
        int sum = 0;
        for (Condition kp : this) sum += kp.hashCode();
        return Objects.hash(getType(), sum);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other)
            return true;

        if (other == null || !getClass().isInstance(other))
            return false;

        MultiCondition oth = (MultiCondition)other;
        if (getType() != oth.getType() || size() != oth.size())
            return false;

        for (int i = 0; i < size(); i++) {
            boolean foundEqual = false;
            for (int j = 0; j < oth.size(); j++) {
                if (get(i).equals(oth.get((i + j) % oth.size()))) {
                    foundEqual = true;
                    break;
                }
            }

            if (!foundEqual)
                return false;
        }

        return true;
    }

    @Override
    public String toString() {
        return toString(getType().toString());
    }

    public String toString(String token) {
        StringBuilder b = new StringBuilder();
        b.append("(");
        for (int i = 0; i < size(); i++) {
            if (i > 0) b.append(" ").append(token).append(" ");
            b.append(get(i));
        }
        b.append(")");
        return b.toString();
    }

}
