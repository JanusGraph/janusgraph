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
import org.apache.commons.lang3.StringUtils;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.schema.Index;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.core.schema.JanusGraphSchemaType;
import org.janusgraph.core.schema.Parameter;
import org.janusgraph.core.schema.json.creator.JsonSchemaCreationContext;
import org.janusgraph.core.schema.json.creator.SchemaCreationException;
import org.janusgraph.core.schema.json.definition.index.AbstractJsonGraphCentricIndexDefinition;
import org.janusgraph.core.schema.json.parser.JsonParameterDefinitionParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractJsonGraphCentricIndexCreator<T extends AbstractJsonGraphCentricIndexDefinition>
        extends AbstractJsonIndexCreator<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractJsonGraphCentricIndexCreator.class);
    private final JsonParameterDefinitionParser jsonParameterDefinitionParser = new JsonParameterDefinitionParser();

    @Override
    protected Index buildIndex(T definition, JsonSchemaCreationContext context) {

        JanusGraphManagement graphManagement = context.getGraphManagement();

        Class<? extends Element> typeClass = toClass(definition.getTypeClass());
        JanusGraphManagement.IndexBuilder indexBuilder = graphManagement.buildIndex(definition.getName(), typeClass);
        addKeys(graphManagement, indexBuilder, definition);
        addIndexOnly(graphManagement, indexBuilder, definition, typeClass);

        return buildSpecificIndex(graphManagement, indexBuilder, definition);
    }

    @Override
    protected boolean containsIndex(T definition, JsonSchemaCreationContext context) {
        return context.getGraphManagement().containsGraphIndex(definition.getName());
    }

    protected void addKeys(JanusGraphManagement graphManagement,
                           JanusGraphManagement.IndexBuilder builder,
                           T definition) {
        if(CollectionUtils.isEmpty(definition.getKeys())){
            return;
        }
        definition.getKeys().forEach(entry -> {
            if (CollectionUtils.isEmpty(entry.getParameters())) {
                builder.addKey(graphManagement.getPropertyKey(entry.getPropertyKey()));
            } else {
                Parameter[] parameters = entry.getParameters().stream()
                    .map(jsonParameterDefinitionParser::parse).toArray(Parameter[]::new);
                builder.addKey(graphManagement.getPropertyKey(entry.getPropertyKey()), parameters);
            }
        });
    }

    protected void addIndexOnly(JanusGraphManagement graphManagement,
                                JanusGraphManagement.IndexBuilder indexBuilder,
                                T definition, Class<? extends Element> typeClass) {
        if(StringUtils.isNotEmpty(definition.getIndexOnly())){
            JanusGraphSchemaType schemaLabel;
            if(Vertex.class.isAssignableFrom(typeClass)){
                schemaLabel = graphManagement.getVertexLabel(definition.getIndexOnly());
            } else if(Edge.class.isAssignableFrom(typeClass)){
                schemaLabel = graphManagement.getEdgeLabel(definition.getIndexOnly());
            } else {
                throw new SchemaCreationException("No implementation for type "+typeClass.getName());
            }
            indexBuilder.indexOnly(schemaLabel);
        }
    }

    private Class<? extends Element> toClass(String type){
        try{
            return (Class<? extends Element>) Class.forName(type);
        } catch (Exception e){
            LOGGER.error("Class [{}] is not a child of {}", type, Element.class.getName());
            throw new SchemaCreationException(e);
        }
    }

    protected abstract Index buildSpecificIndex(JanusGraphManagement graphManagement, JanusGraphManagement.IndexBuilder indexBuilder, T definition);

}
