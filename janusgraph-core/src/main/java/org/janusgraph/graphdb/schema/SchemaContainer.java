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

package org.janusgraph.graphdb.schema;

import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import org.janusgraph.core.EdgeLabel;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.VertexLabel;
import org.janusgraph.core.schema.JanusGraphManagement;

import java.util.Map;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 *
 * @deprecated part of the management revamp in JG, see https://github.com/JanusGraph/janusgraph/projects/3.
 */
@Deprecated
public class SchemaContainer implements SchemaProvider {

    private final Map<String,VertexLabelDefinition> vertexLabels;
    private final Map<String,RelationTypeDefinition> relationTypes;

    public SchemaContainer(JanusGraph graph) {
        vertexLabels = Maps.newHashMap();
        relationTypes = Maps.newHashMap();
        JanusGraphManagement management = graph.openManagement();

        try {
            for (VertexLabel vl : management.getVertexLabels()) {
                VertexLabelDefinition vld = new VertexLabelDefinition(vl);
                vertexLabels.put(vld.getName(),vld);
            }

            for (EdgeLabel el : management.getRelationTypes(EdgeLabel.class)) {
                EdgeLabelDefinition eld = new EdgeLabelDefinition(el);
                relationTypes.put(eld.getName(),eld);
            }
            for (PropertyKey pk : management.getRelationTypes(PropertyKey.class)) {
                PropertyKeyDefinition pkd = new PropertyKeyDefinition(pk);
                relationTypes.put(pkd.getName(), pkd);
            }
        } finally {
            management.rollback();
        }

    }

    public Iterable<VertexLabelDefinition> getVertexLabels() {
        return vertexLabels.values();
    }

    @Override
    public VertexLabelDefinition getVertexLabel(String name) {
        return vertexLabels.get(name);
    }

    public boolean containsVertexLabel(String name) {
        return getVertexLabel(name)!=null;
    }

    public Iterable<PropertyKeyDefinition> getPropertyKeys() {
        return Iterables.filter(relationTypes.values(),PropertyKeyDefinition.class);
    }

    public Iterable<EdgeLabelDefinition> getEdgeLabels() {
        return Iterables.filter(relationTypes.values(),EdgeLabelDefinition.class);
    }

    @Override
    public RelationTypeDefinition getRelationType(String name) {
        return relationTypes.get(name);
    }

    public boolean containsRelationType(String name) {
        return getRelationType(name)!=null;
    }

    @Override
    public EdgeLabelDefinition getEdgeLabel(String name) {
        RelationTypeDefinition def = getRelationType(name);
        if (def!=null && !(def instanceof EdgeLabelDefinition))
            throw new IllegalArgumentException("Not an edge label but property key: " + name);
        return (EdgeLabelDefinition)def;
    }

    @Override
    public PropertyKeyDefinition getPropertyKey(String name) {
        RelationTypeDefinition def = getRelationType(name);
        if (def!=null && !(def instanceof PropertyKeyDefinition))
            throw new IllegalArgumentException("Not a property key but edge label: " + name);
        return (PropertyKeyDefinition)def;
    }

}
