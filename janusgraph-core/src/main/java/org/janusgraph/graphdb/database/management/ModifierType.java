package org.janusgraph.graphdb.database.management;

import org.janusgraph.graphdb.types.TypeDefinitionCategory;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public enum ModifierType {
    CONSISTENCY(TypeDefinitionCategory.CONSISTENCY_LEVEL),
    TTL(TypeDefinitionCategory.TTL);

    private final TypeDefinitionCategory category;

    private ModifierType(final TypeDefinitionCategory category) {
        this.category = category;
    }

    public TypeDefinitionCategory getCategory() {
        return category;
    }
}
