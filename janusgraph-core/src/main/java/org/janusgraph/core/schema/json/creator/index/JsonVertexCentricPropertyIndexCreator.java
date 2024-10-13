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

import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.Index;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.core.schema.json.creator.JsonSchemaCreationContext;
import org.janusgraph.core.schema.json.definition.index.JsonVertexCentricPropertyIndexDefinition;

public class JsonVertexCentricPropertyIndexCreator extends AbstractJsonVertexCentricIndexCreator<JsonVertexCentricPropertyIndexDefinition> {

    @Override
    protected Index buildIndex(JsonVertexCentricPropertyIndexDefinition definition, JsonSchemaCreationContext context){
        JanusGraphManagement graphManagement = context.getGraphManagement();
        PropertyKey propertyKey = graphManagement.getPropertyKey(definition.getIndexedPropertyKey());
        PropertyKey[] propertyKeys = toPropertyKeys(definition, context);

        if (definition.getOrder() == null) {
            return graphManagement.buildPropertyIndex(
                propertyKey,
                definition.getName(),
                propertyKeys
            );
        } else {
            return graphManagement.buildPropertyIndex(
                propertyKey,
                definition.getName(),
                definition.getOrder(),
                propertyKeys
            );
        }
    }

    @Override
    protected String getIndexedElementName(JsonVertexCentricPropertyIndexDefinition definition) {
        return definition.getIndexedPropertyKey();
    }
}
