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

package org.janusgraph.graphdb.tinkerpop.io.graphson;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONTokens;
import org.apache.tinkerpop.gremlin.structure.io.graphson.TinkerPopJacksonModule;
import org.apache.tinkerpop.shaded.jackson.core.JsonGenerationException;
import org.apache.tinkerpop.shaded.jackson.core.JsonGenerator;
import org.apache.tinkerpop.shaded.jackson.core.JsonParser;
import org.apache.tinkerpop.shaded.jackson.core.JsonProcessingException;
import org.apache.tinkerpop.shaded.jackson.core.JsonToken;
import org.apache.tinkerpop.shaded.jackson.core.type.WritableTypeId;
import org.apache.tinkerpop.shaded.jackson.databind.DeserializationContext;
import org.apache.tinkerpop.shaded.jackson.databind.SerializerProvider;
import org.apache.tinkerpop.shaded.jackson.databind.deser.std.StdDeserializer;
import org.apache.tinkerpop.shaded.jackson.databind.jsontype.TypeSerializer;
import org.apache.tinkerpop.shaded.jackson.databind.ser.std.StdSerializer;
import org.janusgraph.core.attribute.Geoshape;
import org.janusgraph.graphdb.relations.RelationIdentifier;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class JanusGraphSONModule extends TinkerPopJacksonModule {

    private static final String TYPE_NAMESPACE = "janusgraph";

    private static final Map<Class, String> TYPE_DEFINITIONS = Collections
            .unmodifiableMap(new LinkedHashMap<Class, String>() {
                {
                    put(RelationIdentifier.class, "RelationIdentifier");
                    put(Geoshape.class, "Geoshape");
                }
            });

    protected JanusGraphSONModule() {
        super("janusgraph");
        addSerializer(RelationIdentifier.class, new RelationIdentifierSerializer());

        addDeserializer(RelationIdentifier.class, new RelationIdentifierDeserializer());
    }

    @Override
    public Map<Class, String> getTypeDefinitions() {
        return TYPE_DEFINITIONS;
    }

    @Override
    public String getTypeNamespace() {
        return TYPE_NAMESPACE;
    }

    public static class RelationIdentifierSerializer extends StdSerializer<RelationIdentifier> {

        public RelationIdentifierSerializer() {
            super(RelationIdentifier.class);
        }

        @Override
        public void serialize(final RelationIdentifier relationIdentifier, final JsonGenerator jsonGenerator,
                final SerializerProvider serializerProvider) throws IOException, JsonGenerationException {
            jsonGenerator.writeString(relationIdentifier.toString());
        }

        @Override
        public void serializeWithType(final RelationIdentifier relationIdentifier, final JsonGenerator jsonGenerator,
                final SerializerProvider serializerProvider, final TypeSerializer typeSerializer)
                throws IOException, JsonProcessingException {
            // since jackson 2.9, must keep track of `typeIdDef` in order to close it properly
            final WritableTypeId typeIdDef = typeSerializer.writeTypePrefix(jsonGenerator, typeSerializer.typeId(relationIdentifier, JsonToken.VALUE_STRING));
            jsonGenerator.writeStartObject();
            jsonGenerator.writeStringField(GraphSONTokens.VALUE, relationIdentifier.toString());
            jsonGenerator.writeStringField(GraphSONTokens.CLASS, HashMap.class.getName());
            jsonGenerator.writeEndObject();
            typeSerializer.writeTypeSuffix(jsonGenerator, typeIdDef);
        }
    }

    public static class RelationIdentifierDeserializer extends StdDeserializer<RelationIdentifier> {
        public RelationIdentifierDeserializer() {
            super(RelationIdentifier.class);
        }

        @Override
        public RelationIdentifier deserialize(final JsonParser jsonParser,
                final DeserializationContext deserializationContext) throws IOException, JsonProcessingException {
            jsonParser.nextToken();
            final Map<String, Object> mapData = deserializationContext.readValue(jsonParser, Map.class);
            return RelationIdentifier.parse((String) mapData.get(GraphSONTokens.VALUE));
        }
    }
}
