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

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.structure.io.graphson.AbstractObjectDeserializer;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONTokens;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONUtil;
import org.apache.tinkerpop.gremlin.structure.io.graphson.TinkerPopJacksonModule;
import org.apache.tinkerpop.shaded.jackson.core.JsonGenerator;
import org.apache.tinkerpop.shaded.jackson.core.JsonParser;
import org.apache.tinkerpop.shaded.jackson.core.JsonToken;
import org.apache.tinkerpop.shaded.jackson.core.type.WritableTypeId;
import org.apache.tinkerpop.shaded.jackson.databind.DeserializationContext;
import org.apache.tinkerpop.shaded.jackson.databind.SerializerProvider;
import org.apache.tinkerpop.shaded.jackson.databind.deser.std.StdDeserializer;
import org.apache.tinkerpop.shaded.jackson.databind.jsontype.TypeSerializer;
import org.apache.tinkerpop.shaded.jackson.databind.ser.std.StdSerializer;
import org.janusgraph.core.attribute.Geoshape;
import org.janusgraph.graphdb.relations.RelationIdentifier;
import org.janusgraph.graphdb.tinkerpop.DeprecatedJanusGraphPSerializer;
import org.janusgraph.graphdb.tinkerpop.JanusGraphPSerializer;
import org.janusgraph.graphdb.tinkerpop.io.JanusGraphP;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author Stephen Mallette (https://stephen.genoprime.com)
 */
public abstract class JanusGraphSONModule extends TinkerPopJacksonModule {

    private static final String TYPE_NAMESPACE = "janusgraph";

    private static final Map<Class, String> TYPE_DEFINITIONS = Collections
            .unmodifiableMap(new LinkedHashMap<Class, String>() {
                {
                    put(RelationIdentifier.class, "RelationIdentifier");
                    put(Geoshape.class, "Geoshape");
                    put(JanusGraphP.class, "JanusGraphP");
                }
            });

    protected JanusGraphSONModule() {
        super("janusgraph");
    }

    @Override
    public Map<Class, String> getTypeDefinitions() {
        return TYPE_DEFINITIONS;
    }

    @Override
    public String getTypeNamespace() {
        return TYPE_NAMESPACE;
    }

    public static class RelationIdentifierSerializerV1d0 extends StdSerializer<RelationIdentifier> {

        public RelationIdentifierSerializerV1d0() {
            super(RelationIdentifier.class);
        }

        @Override
        public void serialize(final RelationIdentifier relationIdentifier, final JsonGenerator jsonGenerator,
                final SerializerProvider serializerProvider) throws IOException {
            jsonGenerator.writeString(relationIdentifier.toString());
        }

        @Override
        public void serializeWithType(final RelationIdentifier relationIdentifier, final JsonGenerator jsonGenerator,
                final SerializerProvider serializerProvider, final TypeSerializer typeSerializer)
                throws IOException {
            // since jackson 2.9, must keep track of `typeIdDef` in order to close it properly
            final WritableTypeId typeIdDef = typeSerializer.writeTypePrefix(jsonGenerator, typeSerializer.typeId(relationIdentifier, JsonToken.VALUE_STRING));
            jsonGenerator.writeStartObject();
            jsonGenerator.writeStringField(GraphSONTokens.VALUE, relationIdentifier.toString());
            jsonGenerator.writeStringField(GraphSONTokens.CLASS, HashMap.class.getName());
            jsonGenerator.writeEndObject();
            typeSerializer.writeTypeSuffix(jsonGenerator, typeIdDef);
        }
    }

    public static class RelationIdentifierDeserializerV1d0 extends StdDeserializer<RelationIdentifier> {
        public RelationIdentifierDeserializerV1d0() {
            super(RelationIdentifier.class);
        }

        @Override
        public RelationIdentifier deserialize(final JsonParser jsonParser,
                final DeserializationContext deserializationContext) throws IOException {
            jsonParser.nextToken();
            final Map<String, Object> mapData = deserializationContext.readValue(jsonParser, Map.class);
            return RelationIdentifier.parse((String) mapData.get(GraphSONTokens.VALUE));
        }
    }

    public static class RelationIdentifierSerializerV2d0 extends StdSerializer<RelationIdentifier> {

        public RelationIdentifierSerializerV2d0() {
            super(RelationIdentifier.class);
        }

        @Override
        public void serialize(final RelationIdentifier relationIdentifier, final JsonGenerator jsonGenerator,
                              final SerializerProvider serializerProvider) throws IOException {
            jsonGenerator.writeString(relationIdentifier.toString());
        }

        @Override
        public void serializeWithType(final RelationIdentifier relationIdentifier, final JsonGenerator jsonGenerator,
                                      final SerializerProvider serializerProvider, final TypeSerializer typeSerializer) throws IOException {

            jsonGenerator.writeStartObject();
            if (typeSerializer != null) jsonGenerator.writeStringField(GraphSONTokens.VALUETYPE, TYPE_NAMESPACE + ":" + TYPE_DEFINITIONS.get(RelationIdentifier.class));
            jsonGenerator.writeFieldName(GraphSONTokens.VALUEPROP);
            GraphSONUtil.writeStartObject(relationIdentifier, jsonGenerator, typeSerializer);
            GraphSONUtil.writeWithType("relationId", relationIdentifier.toString(), jsonGenerator, serializerProvider, typeSerializer);
            GraphSONUtil.writeEndObject(relationIdentifier, jsonGenerator, typeSerializer);
            jsonGenerator.writeEndObject();
        }
    }

    public static class RelationIdentifierDeserializerV2d0 extends AbstractObjectDeserializer<RelationIdentifier> {
        public RelationIdentifierDeserializerV2d0() {
            super(RelationIdentifier.class);
        }

        @Override
        public RelationIdentifier createObject(Map data) {
            return RelationIdentifier.parse((String) data.get("relationId"));
        }
    }

    public static class JanusGraphPSerializerV2d0 extends StdSerializer<JanusGraphP> {
        public JanusGraphPSerializerV2d0() {
            super(JanusGraphP.class);
        }

        @Override
        public void serialize(final JanusGraphP predicate, final JsonGenerator jsonGenerator,
                              final SerializerProvider serializerProvider) throws IOException {
            jsonGenerator.writeString(predicate.toString());
        }

        @Override
        public void serializeWithType(final JanusGraphP value, final JsonGenerator jgen,
                                      final SerializerProvider serializerProvider, final TypeSerializer typeSerializer) throws IOException {
            String predicateName = value.getBiPredicate().toString();
            Object arg = value.getValue();

            jgen.writeStartObject();
            if (typeSerializer != null) jgen.writeStringField(GraphSONTokens.VALUETYPE, TYPE_NAMESPACE + ":" + TYPE_DEFINITIONS.get(JanusGraphP.class));
            jgen.writeFieldName(GraphSONTokens.VALUEPROP);
            GraphSONUtil.writeStartObject(value, jgen, typeSerializer);
            GraphSONUtil.writeWithType(GraphSONTokens.PREDICATE, predicateName, jgen, serializerProvider, typeSerializer);
            GraphSONUtil.writeWithType(GraphSONTokens.VALUE, arg, jgen, serializerProvider, typeSerializer);
            GraphSONUtil.writeEndObject(value, jgen, typeSerializer);
            jgen.writeEndObject();
        }

    }

    public static class JanusGraphPDeserializerV2d0 extends AbstractObjectDeserializer<JanusGraphP> {

        public JanusGraphPDeserializerV2d0() {
            super(JanusGraphP.class);
        }

        @Override
        public JanusGraphP createObject(Map<String, Object> data) {
            String predicate = (String) data.get(GraphSONTokens.PREDICATE);
            Object value = data.get(GraphSONTokens.VALUE);
            return JanusGraphPSerializer.createPredicateWithValue(predicate, value);
        }

        @Override
        public boolean isCachable() {
            return true;
        }
    }

    @Deprecated
    public static class DeprecatedJanusGraphPDeserializerV2d0 extends StdDeserializer<P> {

        public DeprecatedJanusGraphPDeserializerV2d0() {
            super(P.class);
        }

        @Override
        public P deserialize(final JsonParser jsonParser, final DeserializationContext deserializationContext) throws IOException {
            String predicate = null;
            Object value = null;

            while (jsonParser.nextToken() != JsonToken.END_OBJECT) {
                if (jsonParser.getCurrentName().equals(GraphSONTokens.PREDICATE)) {
                    jsonParser.nextToken();
                    predicate = jsonParser.getText();
                } else if (jsonParser.getCurrentName().equals(GraphSONTokens.VALUE)) {
                    jsonParser.nextToken();
                    value = deserializationContext.readValue(jsonParser, Object.class);
                }
            }

            try {
                return DeprecatedJanusGraphPSerializer.createPredicateWithValue(predicate, value);
            } catch (final Exception e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
        }

        @Override
        public boolean isCachable() {
            return true;
        }
    }
}
