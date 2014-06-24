package com.thinkaurelius.titan.core.schema;

import com.thinkaurelius.titan.graphdb.types.TypeDefinitionCategory;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public enum ModifierType {
    TTL(TypeDefinitionCategory.TTL);

    private final TypeDefinitionCategory category;

    private ModifierType(final TypeDefinitionCategory category) {
        this.category = category;
    }

    public TypeDefinitionCategory getCategory() {
        return category;
    }
}
