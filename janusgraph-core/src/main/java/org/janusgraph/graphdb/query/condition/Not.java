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
import org.janusgraph.core.JanusGraphElement;

import java.util.Collections;
import java.util.Objects;

/**
 * Negates the wrapped condition, i.e. semantic NOT
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class Not<E extends JanusGraphElement> implements Condition<E> {

    private final Condition<E> condition;

    public Not(Condition<E> condition) {
        Preconditions.checkNotNull(condition);
        this.condition = condition;
    }

    @Override
    public Type getType() {
        return Type.NOT;
    }

    public Condition<E> getChild() {
        return condition;
    }

    @Override
    public boolean hasChildren() {
        return true;
    }

    @Override
    public int numChildren() {
        return 1;
    }

    @Override
    public boolean evaluate(E element) {
        return !condition.evaluate(element);
    }

    @Override
    public Iterable<Condition<E>> getChildren() {
        return Collections.singletonList(condition);
    }

    @Override
    public int hashCode() {
        return Objects.hash(getType(), condition);
    }

    @Override
    public boolean equals(Object other) {
        return this == other || !(other == null || !getClass().isInstance(other)) && condition.equals(((Not) other).condition);

    }

    @Override
    public String toString() {
        return "!("+ condition.toString()+")";
    }

    public static <E extends JanusGraphElement> Not<E> of(Condition<E> element) {
        return new Not<>(element);
    }

}
