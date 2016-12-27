package com.thinkaurelius.titan.graphdb.types;

import com.google.common.base.Preconditions;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class TypeDefinitionDescription {

    private TypeDefinitionCategory category;
    private Object modifier;

    public TypeDefinitionDescription(TypeDefinitionCategory category, Object modifier) {
        Preconditions.checkNotNull(category);
        if (category.isProperty()) Preconditions.checkArgument(modifier==null);
        else {
            Preconditions.checkArgument(category.isEdge());
            if (category.hasDataType()) Preconditions.checkArgument(modifier==null || modifier.getClass().equals(category.getDataType()));
            else Preconditions.checkArgument(modifier==null);
        }
        this.category = category;
        this.modifier = modifier;
    }

    public static TypeDefinitionDescription of(TypeDefinitionCategory category) {
        return new TypeDefinitionDescription(category,null);
    }

    public TypeDefinitionCategory getCategory() {
        return category;
    }

    public Object getModifier() {
        return modifier;
    }
}
