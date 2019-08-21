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

import org.janusgraph.core.JanusGraphEdge;
import org.janusgraph.core.JanusGraphRelation;
import org.janusgraph.core.JanusGraphVertex;

import org.apache.tinkerpop.gremlin.structure.Direction;

import com.google.common.base.Preconditions;

import java.util.Objects;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class IncidenceDirectionCondition<E extends JanusGraphRelation> extends Literal<E> {

    private final Direction direction;
    private final JanusGraphVertex otherVertex;

    public IncidenceDirectionCondition(Direction direction, JanusGraphVertex otherVertex) {
        Preconditions.checkNotNull(direction);
        Preconditions.checkNotNull(otherVertex);
        this.direction = direction;
        this.otherVertex = otherVertex;
    }

    @Override
    public boolean evaluate(E relation) {
        return relation.isEdge() && ((JanusGraphEdge) relation).vertex(direction).equals(otherVertex);
    }

    @Override
    public int hashCode() {
        return Objects.hash(getType(), direction, otherVertex);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other)
            return true;

        if (other==null || !getClass().isInstance(other))
            return false;

        IncidenceDirectionCondition oth = (IncidenceDirectionCondition)other;
        return direction==oth.direction && otherVertex.equals(oth.otherVertex);
    }

    @Override
    public String toString() {
        return "incidence["+ direction + "-" + otherVertex + "]";
    }
}
