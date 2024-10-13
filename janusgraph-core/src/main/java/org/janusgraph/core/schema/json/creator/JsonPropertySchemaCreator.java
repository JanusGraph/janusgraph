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

package org.janusgraph.core.schema.json.creator;

import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.core.schema.PropertyKeyMaker;
import org.janusgraph.core.schema.json.definition.JsonPropertyKeyDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

public class JsonPropertySchemaCreator implements JsonSchemaCreator<JsonPropertyKeyDefinition> {

    private static final Logger LOGGER = LoggerFactory.getLogger(JsonPropertySchemaCreator.class);

    @Override
    public boolean create(JsonPropertyKeyDefinition definition, JsonSchemaCreationContext context) {

        JanusGraphManagement graphManagement = context.getGraphManagement();

        if(isPropertyExists(graphManagement, definition)) {
            LOGGER.info("Creation of the property {} was skipped because it is already exists.", definition.getKey());
            return false;
        }

        PropertyKey propertyKey = createProperty(graphManagement, definition);
        if(definition.getTtl() != null){
            graphManagement.setTTL(propertyKey, Duration.ofMillis(definition.getTtl()));
        }
        if(definition.getConsistency() != null){
            graphManagement.setConsistency(propertyKey, definition.getConsistency());
        }

        LOGGER.info("Property {} was created", definition.getKey());

        return true;
    }

    private PropertyKey createProperty(JanusGraphManagement graphManagement,
                                       JsonPropertyKeyDefinition definition) {

        Class propertyTypeClass;
        try{
            propertyTypeClass = Class.forName(definition.getClassName());
        } catch (Exception e){
            throw new SchemaCreationException(e);
        }

        PropertyKeyMaker propertyKeyMaker = graphManagement.makePropertyKey(definition.getKey())
                .dataType(propertyTypeClass);

        if(definition.getCardinality() != null){
            propertyKeyMaker.cardinality(definition.getCardinality());
        }

        return propertyKeyMaker.make();
    }

    private boolean isPropertyExists(JanusGraphManagement graphManagement,
                                     JsonPropertyKeyDefinition definition) {
        return graphManagement.containsPropertyKey(definition.getKey());
    }

}
