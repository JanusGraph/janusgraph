package com.thinkaurelius.titan.graphdb.schema;

import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.thinkaurelius.titan.core.EdgeLabel;
import com.thinkaurelius.titan.core.PropertyKey;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.VertexLabel;
import com.thinkaurelius.titan.core.schema.TitanManagement;

import java.util.Map;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class SchemaContainer implements SchemaProvider {

    private final Map<String,VertexLabelDefinition> vertexLabels;
    private final Map<String,RelationTypeDefinition> relationTypes;

    public SchemaContainer(TitanGraph graph) {
        vertexLabels = Maps.newHashMap();
        relationTypes = Maps.newHashMap();
        TitanManagement mgmt = graph.openManagement();

        try {
            for (VertexLabel vl : mgmt.getVertexLabels()) {
                VertexLabelDefinition vld = new VertexLabelDefinition(vl);
                vertexLabels.put(vld.getName(),vld);
            }

            for (EdgeLabel el : mgmt.getRelationTypes(EdgeLabel.class)) {
                EdgeLabelDefinition eld = new EdgeLabelDefinition(el);
                relationTypes.put(eld.getName(),eld);
            }
            for (PropertyKey pk : mgmt.getRelationTypes(PropertyKey.class)) {
                PropertyKeyDefinition pkd = new PropertyKeyDefinition(pk);
                relationTypes.put(pkd.getName(), pkd);
            }
        } finally {
            mgmt.rollback();
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
