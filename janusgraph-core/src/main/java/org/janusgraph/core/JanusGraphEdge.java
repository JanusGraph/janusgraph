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


package org.janusgraph.core;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Iterator;
import java.util.stream.Stream;

/**
 * A JanusGraphEdge connects two {@link JanusGraphVertex}. It extends the functionality provided by Blueprint's {@link Edge} and
 * is a special case of a {@link JanusGraphRelation}.
 *
 * @author Matthias Br&ouml;cheler (http://www.matthiasb.com)
 * @see Edge
 * @see JanusGraphRelation
 * @see EdgeLabel
 */
public interface JanusGraphEdge extends JanusGraphRelation, Edge {

    /**
     * Returns the edge label of this edge
     *
     * @return edge label of this edge
     */
    default EdgeLabel edgeLabel() {
        assert getType() instanceof EdgeLabel;
        return (EdgeLabel)getType();
    }

    /**
     * Returns the vertex for the specified direction.
     * The direction cannot be Direction.BOTH.
     *
     * @param dir Direction of IN or OUT
     * @return the vertex for the specified direction
     */
    JanusGraphVertex vertex(Direction dir);

    @Override
    default JanusGraphVertex outVertex() {
        return vertex(Direction.OUT);
    }

    @Override
    default JanusGraphVertex inVertex() {
        return vertex(Direction.IN);
    }


    /**
     * Returns the vertex at the opposite end of the edge.
     *
     * @param vertex vertex on which this edge is incident
     * @return The vertex at the opposite end of the edge.
     * @throws InvalidElementException if the edge is not incident on the specified vertex
     */
    JanusGraphVertex otherVertex(Vertex vertex);


    @Override
    default Iterator<Vertex> vertices(Direction direction) {
        if (direction==Direction.BOTH) {
            return Stream.of((Vertex) vertex(Direction.OUT), vertex(Direction.IN)).iterator();
        }
        return Stream.of((Vertex) vertex(direction)).iterator();
    }

}
