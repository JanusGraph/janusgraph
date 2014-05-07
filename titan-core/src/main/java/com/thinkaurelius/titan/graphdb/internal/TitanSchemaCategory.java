package com.thinkaurelius.titan.graphdb.internal;

import com.thinkaurelius.titan.graphdb.types.TypeDefinitionCategory;
import com.thinkaurelius.titan.graphdb.types.TypeDefinitionMap;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public enum TitanSchemaCategory {

    LABEL, KEY, INDEX, MODIFIER;


    public boolean isRelationType() {
        return this==LABEL || this==KEY;
    }

    public boolean hasName() {
        switch(this) {
            case LABEL:
            case KEY:
            case INDEX:
                return true;
            case MODIFIER:
                return false;
            default: throw new AssertionError();
        }
    }

    public void verifyValidDefinition(TypeDefinitionMap definition) {

        switch(this) {
            case LABEL:
                definition.isValidDefinition(TypeDefinitionCategory.EDGE_LABEL_DEFINITION_CATEGORIES);
                break;
            case KEY:
                definition.isValidDefinition(TypeDefinitionCategory.PROPERTY_KEY_DEFINITION_CATEGORIES);
                break;
            case INDEX:
                definition.isValidDefinition(TypeDefinitionCategory.INDEX_DEFINITION_CATEGORIES);
                break;
            case MODIFIER:
                definition.isValidDefinition(TypeDefinitionCategory.CONSISTENCY_MODIFIER_DEFINITION_CATEGORIES);
                break;
            default: throw new AssertionError();
        }
    }


}
