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

import java.util.Objects;

/**
 * A fixed valued literal, which always returns either true or false irrespective of the element which is evaluated.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class FixedCondition<E extends JanusGraphElement> extends Literal<E> {

    private final boolean value;

    public FixedCondition(final boolean value) {
        this.value = value;
    }

    @Override
    public boolean evaluate(E element) {
        return value;
    }

    @Override
    public int hashCode() {
        return Objects.hash(getType(), value);
    }

    @Override
    public boolean equals(Object other) {
        return this == other || !(other == null || !getClass().isInstance(other)) && value == ((FixedCondition) other).value;

    }

    @Override
    public String toString() {
        return String.valueOf(value);
    }
}
