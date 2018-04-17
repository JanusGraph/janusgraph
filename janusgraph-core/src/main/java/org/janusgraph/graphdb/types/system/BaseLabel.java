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

package org.janusgraph.graphdb.types.system;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.janusgraph.core.Connection;
import org.janusgraph.core.EdgeLabel;
import org.janusgraph.core.Multiplicity;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.graphdb.internal.JanusGraphSchemaCategory;

import java.util.ArrayList;
import java.util.Collection;

public class BaseLabel extends BaseRelationType implements EdgeLabel {

    public static final BaseLabel SchemaDefinitionEdge =
            new BaseLabel("SchemaRelated", 36, Direction.BOTH, Multiplicity.MULTI);

    public static final BaseLabel VertexLabelEdge =
            new BaseLabel("vertexlabel", 2, Direction.OUT, Multiplicity.MANY2ONE);


    private final Direction directionality;
    private final Multiplicity multiplicity;

    private BaseLabel(String name, int id, Direction uniDirectionality, Multiplicity multiplicity) {
        super(name, id, JanusGraphSchemaCategory.EDGELABEL);
        this.directionality = uniDirectionality;
        this.multiplicity = multiplicity;
    }

    @Override
    public long[] getSignature() {
        return new long[]{BaseKey.SchemaDefinitionDesc.longId()};
    }

    @Override
    public Multiplicity multiplicity() {
        return multiplicity;
    }

    @Override
    public Collection<PropertyKey> mappedProperties() {
        return new ArrayList<>();
    }

    @Override
    public Collection<Connection> mappedConnections() {
        return new ArrayList<>();
    }

    @Override
    public final boolean isPropertyKey() {
        return false;
    }

    @Override
    public final boolean isEdgeLabel() {
        return true;
    }

    @Override
    public boolean isDirected() {
        return true;
    }

    @Override
    public boolean isUnidirected() {
        return isUnidirected(Direction.OUT);
    }

    @Override
    public boolean isUnidirected(Direction dir) {
        return dir== directionality;
    }


}
