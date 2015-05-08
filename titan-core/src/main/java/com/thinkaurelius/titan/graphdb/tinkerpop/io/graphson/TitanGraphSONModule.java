package com.thinkaurelius.titan.graphdb.tinkerpop.io.graphson;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.SerializerProvider;
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
            long[] asLong = relationIdentifier.getLongRepresentation();
            //  0=relationId, 1=outVertexId, 2=typeId, 3=inVertexId (optional)
            jsonGenerator.writeNumberField("relationId",asLong[0]);
            jsonGenerator.writeNumberField("outVertexId", asLong[1]);
            jsonGenerator.writeNumberField("typeId",asLong[2]);
            if (asLong.length>3) jsonGenerator.writeNumberField("inVertexId",asLong[3]);
            jsonGenerator.writeEndObject();
        }


    }
}
