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

import org.apache.commons.collections.CollectionUtils;
import org.janusgraph.core.schema.json.creator.index.JsonCompositeIndexCreator;
import org.janusgraph.core.schema.json.creator.index.JsonMixedIndexCreator;
import org.janusgraph.core.schema.json.creator.index.JsonVertexCentricEdgeIndexCreator;
import org.janusgraph.core.schema.json.creator.index.JsonVertexCentricPropertyIndexCreator;
import org.janusgraph.core.schema.json.definition.JsonEdgeLabelDefinition;
import org.janusgraph.core.schema.json.definition.JsonPropertyKeyDefinition;
import org.janusgraph.core.schema.json.definition.JsonSchemaDefinition;
import org.janusgraph.core.schema.json.definition.JsonVertexLabelDefinition;
import org.janusgraph.core.schema.json.definition.index.JsonCompositeIndexDefinition;
import org.janusgraph.core.schema.json.definition.index.JsonMixedIndexDefinition;
import org.janusgraph.core.schema.json.definition.index.JsonVertexCentricEdgeIndexDefinition;
import org.janusgraph.core.schema.json.definition.index.JsonVertexCentricPropertyIndexDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GeneralJsonSchemaCreator implements JsonSchemaCreator<JsonSchemaDefinition>{

    private static final Logger LOGGER = LoggerFactory.getLogger(GeneralJsonSchemaCreator.class);

    private final JsonPropertySchemaCreator propertySchemaCreator = new JsonPropertySchemaCreator();
    private final JsonVertexSchemaCreator vertexSchemaCreator = new JsonVertexSchemaCreator();
    private final JsonEdgeSchemaCreator edgeSchemaCreator = new JsonEdgeSchemaCreator();
    private final JsonCompositeIndexCreator compositeIndexCreator = new JsonCompositeIndexCreator();
    private final JsonMixedIndexCreator mixedIndexCreator = new JsonMixedIndexCreator();
    private final JsonVertexCentricEdgeIndexCreator vertexCentricEdgeIndexCreator = new JsonVertexCentricEdgeIndexCreator();
    private final JsonVertexCentricPropertyIndexCreator vertexCentricPropertyIndexCreator = new JsonVertexCentricPropertyIndexCreator();

    @Override
    public boolean create(JsonSchemaDefinition generalDefinition, JsonSchemaCreationContext context) {

        LOGGER.info("Starting general schema initialization.");

        boolean wasUpdated = false;

        if(context.isCreateSchemaElements()){
            // Create all properties
            if(CollectionUtils.isNotEmpty(generalDefinition.getPropertyKeys())){
                for(JsonPropertyKeyDefinition definition : generalDefinition.getPropertyKeys()){
                    wasUpdated |= propertySchemaCreator.create(definition, context);
                }
            }

            // Create all vertices
            if(CollectionUtils.isNotEmpty(generalDefinition.getVertexLabels())){
                for(JsonVertexLabelDefinition definition : generalDefinition.getVertexLabels()){
                    wasUpdated |= vertexSchemaCreator.create(definition, context);
                }
            }

            // Create all edges
            if(CollectionUtils.isNotEmpty(generalDefinition.getEdgeLabels())){
                for(JsonEdgeLabelDefinition definition : generalDefinition.getEdgeLabels()){
                    wasUpdated |= edgeSchemaCreator.create(definition, context);
                }
            }
        }

        if(context.isCreateIndices()){
            // Create all composite indices
            if(CollectionUtils.isNotEmpty(generalDefinition.getCompositeIndexes())){
                for(JsonCompositeIndexDefinition definition : generalDefinition.getCompositeIndexes()){
                    wasUpdated |= compositeIndexCreator.create(definition, context);
                }
            }

            // Create all mixed indices
            if(CollectionUtils.isNotEmpty(generalDefinition.getMixedIndexes())){
                for(JsonMixedIndexDefinition definition : generalDefinition.getMixedIndexes()){
                    wasUpdated |= mixedIndexCreator.create(definition, context);
                }
            }

            // Create all vertex-centric edge indices
            if(CollectionUtils.isNotEmpty(generalDefinition.getVertexCentricEdgeIndexes())){
                for(JsonVertexCentricEdgeIndexDefinition definition : generalDefinition.getVertexCentricEdgeIndexes()){
                    wasUpdated |= vertexCentricEdgeIndexCreator.create(definition, context);
                }
            }

            // Create all vertex-centric property indices
            if(CollectionUtils.isNotEmpty(generalDefinition.getVertexCentricPropertyIndexes())){
                for(JsonVertexCentricPropertyIndexDefinition definition : generalDefinition.getVertexCentricPropertyIndexes()){
                    wasUpdated |= vertexCentricPropertyIndexCreator.create(definition, context);
                }
            }
        }

        if(wasUpdated){
            LOGGER.info("Schema initialization complete.");
        } else {
            LOGGER.info("There was no any changes during schema initialization. Schema initialization is skipped.");
        }

        return wasUpdated;
    }
}
