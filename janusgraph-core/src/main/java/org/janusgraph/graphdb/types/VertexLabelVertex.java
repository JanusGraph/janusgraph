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

package org.janusgraph.graphdb.types;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.janusgraph.core.Connection;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.VertexLabel;
import org.janusgraph.graphdb.internal.InternalVertexLabel;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.janusgraph.graphdb.types.vertices.JanusGraphSchemaVertex;
import org.janusgraph.graphdb.util.CollectionsUtil;

import java.util.Collection;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class VertexLabelVertex extends JanusGraphSchemaVertex implements InternalVertexLabel {

    private Integer ttl = null;

    public VertexLabelVertex(StandardJanusGraphTx tx, long id, byte lifecycle) {
        super(tx, id, lifecycle);
    }

    @Override
    public boolean isPartitioned() {
        return getDefinition().getValue(TypeDefinitionCategory.PARTITIONED, Boolean.class);
    }

    @Override
    public boolean isStatic() {
        return getDefinition().getValue(TypeDefinitionCategory.STATIC, Boolean.class);
    }

    @Override
    public Collection<PropertyKey> mappedProperties() {
        return CollectionsUtil.toArrayList(
            getRelated(TypeDefinitionCategory.PROPERTY_KEY_EDGE, Direction.OUT),
            entry -> (PropertyKey) entry.getSchemaType()
        );
    }

    @Override
    public Collection<Connection> mappedConnections() {
        return CollectionsUtil.toArrayList(
            getRelated(TypeDefinitionCategory.CONNECTION_EDGE, Direction.OUT),
            entry -> new Connection((String) entry.getModifier(), this, (VertexLabel) entry.getSchemaType())
        );
    }

    @Override
    public boolean hasDefaultConfiguration() {
        return !isPartitioned() && !isStatic();
    }

    @Override
    public int getTTL() {
        if (null == ttl) {
            ttl = TypeUtil.getTTL(this);
        }
        return ttl;
    }

}
