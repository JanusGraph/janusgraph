package com.thinkaurelius.titan.graphdb.tinkerpop.io.graphson;

import com.thinkaurelius.titan.core.attribute.Geoshape;
import com.thinkaurelius.titan.graphdb.relations.RelationIdentifier;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONTokens;
import org.apache.tinkerpop.shaded.jackson.core.JsonGenerationException;
import org.apache.tinkerpop.shaded.jackson.core.JsonGenerator;
import org.apache.tinkerpop.shaded.jackson.core.JsonParser;
import org.apache.tinkerpop.shaded.jackson.core.JsonProcessingException;
import org.apache.tinkerpop.shaded.jackson.databind.DeserializationContext;
import org.apache.tinkerpop.shaded.jackson.databind.SerializerProvider;
import org.apache.tinkerpop.shaded.jackson.databind.deser.std.StdDeserializer;
import org.apache.tinkerpop.shaded.jackson.databind.jsontype.TypeSerializer;
import org.apache.tinkerpop.shaded.jackson.databind.module.SimpleModule;
import org.apache.tinkerpop.shaded.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class TitanGraphSONModule extends SimpleModule {

    private static final String FIELD_RELATION_ID = "relationId";

    private TitanGraphSONModule() {
        addSerializer(RelationIdentifier.class, new RelationIdentifierSerializer());
        addSerializer(Geoshape.class, new Geoshape.GeoshapeGsonSerializer());

        addDeserializer(RelationIdentifier.class, new RelationIdentifierDeserializer());
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
            jsonGenerator.writeStartObject();
            jsonGenerator.writeStringField(GraphSONTokens.CLASS, RelationIdentifier.class.getName());
            jsonGenerator.writeStringField(FIELD_RELATION_ID, relationIdentifier.toString());
            jsonGenerator.writeEndObject();
        }
    }

    public static class RelationIdentifierDeserializer extends StdDeserializer<RelationIdentifier> {
        public RelationIdentifierDeserializer() {
            super(RelationIdentifier.class);
        }

        @Override
        public RelationIdentifier deserialize(final JsonParser jsonParser, final DeserializationContext deserializationContext) throws IOException, JsonProcessingException {
            if (!jsonParser.getText().equals(FIELD_RELATION_ID)) throw new IOException(String.format("Invalid serialization format for %s", RelationIdentifier.class));
            final RelationIdentifier ri = RelationIdentifier.parse(jsonParser.nextTextValue());
            jsonParser.nextToken();
            return ri;
        }
    }
}
