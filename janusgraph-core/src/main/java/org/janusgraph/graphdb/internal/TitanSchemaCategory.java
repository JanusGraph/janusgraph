package com.thinkaurelius.titan.graphdb.internal;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.graphdb.types.TypeDefinitionCategory;
import com.thinkaurelius.titan.graphdb.types.TypeDefinitionMap;
import com.thinkaurelius.titan.graphdb.types.TypeUtil;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.commons.lang.StringUtils;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public enum TitanSchemaCategory {

    EDGELABEL, PROPERTYKEY, VERTEXLABEL, GRAPHINDEX, TYPE_MODIFIER;


    public boolean isRelationType() {
        return this== EDGELABEL || this== PROPERTYKEY;
    }

    public boolean hasName() {
        switch(this) {
            case EDGELABEL:
            case PROPERTYKEY:
            case GRAPHINDEX:
            case VERTEXLABEL:
                return true;
            case TYPE_MODIFIER:
                return false;
            default: throw new AssertionError();
        }
    }

    public String getSchemaName(String name) {
        Preconditions.checkState(hasName());
        TypeUtil.checkTypeName(this,name);
        String prefix;
        switch(this) {
            case EDGELABEL:
            case PROPERTYKEY:
                prefix = "rt";
                break;
            case GRAPHINDEX:
                prefix = "gi";
                break;
            case VERTEXLABEL:
                prefix = "vl";
                break;
            default: throw new AssertionError();
        }
        return Token.getSeparatedName(prefix,name);
    }

    public static String getRelationTypeName(String name) {
        return EDGELABEL.getSchemaName(name);
    }

    public static String getName(String schemaName) {
        String[] comps = Token.splitSeparatedName(schemaName);
        Preconditions.checkArgument(comps.length==2);
        return comps[1];
    }

    public void verifyValidDefinition(TypeDefinitionMap definition) {

        switch(this) {
            case EDGELABEL:
                definition.isValidDefinition(TypeDefinitionCategory.EDGELABEL_DEFINITION_CATEGORIES);
                break;
            case PROPERTYKEY:
                definition.isValidDefinition(TypeDefinitionCategory.PROPERTYKEY_DEFINITION_CATEGORIES);
                break;
            case GRAPHINDEX:
                definition.isValidDefinition(TypeDefinitionCategory.INDEX_DEFINITION_CATEGORIES);
                break;
            case TYPE_MODIFIER:
                definition.isValidTypeModifierDefinition(TypeDefinitionCategory.TYPE_MODIFIER_DEFINITION_CATEGORIES);
                break;
            case VERTEXLABEL:
                definition.isValidDefinition(TypeDefinitionCategory.VERTEXLABEL_DEFINITION_CATEGORIES);
                break;
            default: throw new AssertionError();
        }
    }


}
