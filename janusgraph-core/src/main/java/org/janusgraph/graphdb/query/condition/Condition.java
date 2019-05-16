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

/**
 * A logical condition which evaluates against a provided element to true or false.
 * <p>
 * A condition can be nested to form complex logical expressions with AND, OR and NOT.
 * A condition is either a literal, a negation of a condition, or a logical combination of conditions (AND, OR).
 * If a condition has sub-conditions we consider those to be children.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface Condition<E extends JanusGraphElement> {

    enum Type { AND, OR, NOT, LITERAL}

    Type getType();

    Iterable<Condition<E>> getChildren();

    boolean hasChildren();

    int numChildren();

    boolean evaluate(E element);

    int hashCode();

    boolean equals(Object other);

    String toString();

}
