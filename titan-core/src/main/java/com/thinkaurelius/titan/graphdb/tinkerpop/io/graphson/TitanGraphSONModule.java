package com.thinkaurelius.titan.graphdb.tinkerpop.io.graphson;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.thinkaurelius.titan.core.attribute.Geoshape;
import com.thinkaurelius.titan.graphdb.relations.RelationIdentifier;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONTokens;

import java.io.IOException;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class TitanGraphSONModule extends SimpleModule {

    private TitanGraphSONModule() {
        addSerializer(RelationIdentifier.class, new RelationIdentifierSerializer());
        addSerializer(Geoshape.class, new Geoshape.GeoshapeGsonSerializer());

        addDeserializer(RelationIdentifier.class, new RelationIdentifierDeserializer());
        addDeserializer(Geoshape.class, new Geoshape.GeoshapeGsonDeserializer());
    }

    private static final TitanGraphSONModule INSTANCE = new TitanGraphSONModule();

    public static final TitanGraphSONModule getInstance() {
        return INSTANCE;
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
            jsonGenerator.writeStartArray();
            jsonGenerator.writeString(RelationIdentifier.class.getName());
            jsonGenerator.writeString(relationIdentifier.toString());
            jsonGenerator.writeEndArray();
        }
    }

    public static class RelationIdentifierDeserializer extends StdDeserializer<RelationIdentifier> {
        public RelationIdentifierDeserializer() {
            super(RelationIdentifier.class);
        }

        @Override
        public RelationIdentifier deserialize(final JsonParser jsonParser, final DeserializationContext deserializationContext) throws IOException, JsonProcessingException {
            return RelationIdentifier.parse(jsonParser.getValueAsString());
        }
    }
}
