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

import org.janusgraph.core.attribute.Geoshape;
import org.janusgraph.graphdb.relations.RelationIdentifier;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONTokens;
import org.apache.tinkerpop.gremlin.structure.io.graphson.TinkerPopJacksonModule;
import org.apache.tinkerpop.shaded.jackson.core.JsonGenerationException;
import org.apache.tinkerpop.shaded.jackson.core.JsonGenerator;
import org.apache.tinkerpop.shaded.jackson.core.JsonParser;
import org.apache.tinkerpop.shaded.jackson.core.JsonProcessingException;
import org.apache.tinkerpop.shaded.jackson.databind.DeserializationContext;
import org.apache.tinkerpop.shaded.jackson.databind.SerializerProvider;
import org.apache.tinkerpop.shaded.jackson.databind.deser.std.StdDeserializer;
import org.apache.tinkerpop.shaded.jackson.databind.jsontype.TypeSerializer;
import org.apache.tinkerpop.shaded.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class JanusGraphSONModule extends TinkerPopJacksonModule {

    private static final String TYPE_NAMESPACE = "janusgraph";

    private static final Map<Class, String> TYPE_DEFINITIONS = Collections.unmodifiableMap(
            new LinkedHashMap<Class, String>() {{
                put(RelationIdentifier.class, "RelationIdentifier");
                put(Geoshape.class, "Geoshape");
            }});

    private JanusGraphSONModule() {
        super("janusgraph");
        addSerializer(RelationIdentifier.class, new RelationIdentifierSerializer());
        addSerializer(Geoshape.class, new Geoshape.GeoshapeGsonSerializer());

        addDeserializer(RelationIdentifier.class, new RelationIdentifierDeserializer());
        addDeserializer(Geoshape.class, new Geoshape.GeoshapeGsonDeserializer());
    }

    private static final JanusGraphSONModule INSTANCE = new JanusGraphSONModule();

    public static final JanusGraphSONModule getInstance() {
        return INSTANCE;
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
                                      final SerializerProvider serializerProvider, final TypeSerializer typeSerializer) throws IOException, JsonProcessingException {
            typeSerializer.writeTypePrefixForScalar(relationIdentifier, jsonGenerator);
            jsonGenerator.writeStartObject();
            jsonGenerator.writeStringField(GraphSONTokens.VALUE, relationIdentifier.toString());
            jsonGenerator.writeStringField(GraphSONTokens.CLASS, HashMap.class.getName());
            jsonGenerator.writeEndObject();
            typeSerializer.writeTypeSuffixForScalar(relationIdentifier, jsonGenerator);
        }
    }

    public static class RelationIdentifierDeserializer extends StdDeserializer<RelationIdentifier> {
        public RelationIdentifierDeserializer() {
            super(RelationIdentifier.class);
        }

        @Override
        public RelationIdentifier deserialize(final JsonParser jsonParser, final DeserializationContext deserializationContext) throws IOException, JsonProcessingException {
            jsonParser.nextToken();
            final Map<String, Object> mapData = deserializationContext.readValue(jsonParser, Map.class);
            return RelationIdentifier.parse((String) mapData.get(GraphSONTokens.VALUE));
        }
    }
}
