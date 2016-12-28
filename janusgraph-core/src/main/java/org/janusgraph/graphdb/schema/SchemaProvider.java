package com.thinkaurelius.titan.graphdb.schema;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface SchemaProvider {

    public EdgeLabelDefinition getEdgeLabel(String name);

    public PropertyKeyDefinition getPropertyKey(String name);

    public RelationTypeDefinition getRelationType(String name);

    public VertexLabelDefinition getVertexLabel(String name);

}
