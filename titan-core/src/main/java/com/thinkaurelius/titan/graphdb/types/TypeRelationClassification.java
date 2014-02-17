package com.thinkaurelius.titan.graphdb.types;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class TypeRelationClassification {

    private final TypeAttributeType type;
    private final Object modifier;

    public TypeRelationClassification(TypeAttributeType type, Object modifier) {
        this.type = type;
        this.modifier = modifier;
    }
}
