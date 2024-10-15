// Copyright 2024 JanusGraph Authors
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

package org.janusgraph.core.schema.json.creator.index;

import org.apache.commons.collections.CollectionUtils;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.RelationType;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.core.schema.json.creator.JsonSchemaCreationContext;
import org.janusgraph.core.schema.json.definition.index.AbstractJsonVertexCentricIndexDefinition;

public abstract class AbstractJsonVertexCentricIndexCreator
        <T extends AbstractJsonVertexCentricIndexDefinition> extends AbstractJsonIndexCreator<T> {

    private static final PropertyKey[] EMPTY_PROPERTY_KEYS = new PropertyKey[0];

    @Override
    protected boolean containsIndex(T definition, JsonSchemaCreationContext context) {
        JanusGraphManagement graphManagement = context.getGraphManagement();
        RelationType relationType = graphManagement.getRelationType(getIndexedElementName(definition));
        return graphManagement.containsRelationIndex(relationType, definition.getName());
    }

    protected PropertyKey[] toPropertyKeys(T definition, JsonSchemaCreationContext context){
        if(CollectionUtils.isEmpty(definition.getPropertyKeys())){
            return EMPTY_PROPERTY_KEYS;
        }
        return definition.getPropertyKeys().stream()
                .map(propertyKey -> context.getGraphManagement().getPropertyKey(propertyKey))
                .toArray(PropertyKey[]::new);
    }

    protected abstract String getIndexedElementName(T definition);

}
