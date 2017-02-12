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

package org.janusgraph.graphdb.internal;

import com.google.common.base.Preconditions;
import org.janusgraph.graphdb.types.TypeDefinitionCategory;
import org.janusgraph.graphdb.types.TypeDefinitionMap;
import org.janusgraph.graphdb.types.TypeUtil;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public enum JanusGraphSchemaCategory {

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
