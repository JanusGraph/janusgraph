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

package org.janusgraph.graphdb.types.vertices;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.janusgraph.core.Connection;
import org.janusgraph.core.EdgeLabel;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.VertexLabel;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.janusgraph.graphdb.types.SchemaSource;
import org.janusgraph.graphdb.types.TypeDefinitionCategory;
import org.janusgraph.graphdb.types.TypeDefinitionDescription;
import org.janusgraph.graphdb.types.system.BaseKey;
import org.janusgraph.graphdb.types.system.BaseLabel;

import java.util.ArrayList;
import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class EdgeLabelVertex extends RelationTypeVertex implements EdgeLabel {

    public EdgeLabelVertex(StandardJanusGraphTx tx, long id, byte lifecycle) {
        super(tx, id, lifecycle);
    }

    @Override
    public boolean isDirected() {
        return isUnidirected(Direction.BOTH);
    }

    @Override
    public boolean isUnidirected() {
        return isUnidirected(Direction.OUT);

    }

    @Override
    public Collection<PropertyKey> mappedProperties() {
        return StreamSupport.stream(getRelated(TypeDefinitionCategory.PROPERTY_KEY_EDGE, Direction.OUT).spliterator(), false)
            .map(entry -> (PropertyKey) entry.getSchemaType())
            .collect(Collectors.toList());
    }

    @Override
    public Collection<Connection> mappedConnections() {
        String name = name();
        return StreamSupport.stream(getRelated(TypeDefinitionCategory.UPDATE_CONNECTION_EDGE, Direction.OUT).spliterator(), false)
            .map(entry -> (JanusGraphSchemaVertex) entry.getSchemaType())
            .flatMap(s -> StreamSupport.stream(s.getEdges(TypeDefinitionCategory.CONNECTION_EDGE, Direction.OUT).spliterator(), false))
            .map(Connection::new)
            .filter(s -> s.getEdgeLabel().equals(name))
            .collect(Collectors.toList());
    }

    @Override
    public boolean isUnidirected(Direction dir) {
        return getDefinition().getValue(TypeDefinitionCategory.UNIDIRECTIONAL,Direction.class)==dir;
    }

    @Override
    public final boolean isPropertyKey() {
        return false;
    }

    @Override
    public final boolean isEdgeLabel() {
        return true;
    }
}
