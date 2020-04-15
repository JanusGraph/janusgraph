// Copyright 2020 JanusGraph Authors
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

package org.janusgraph.graphdb.tinkerpop.io.binary;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.apache.tinkerpop.gremlin.driver.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.driver.ser.GraphBinaryMessageSerializerV1;
import org.apache.tinkerpop.gremlin.driver.ser.NettyBufferFactory;
import org.apache.tinkerpop.gremlin.structure.io.Buffer;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryReader;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryWriter;
import org.apache.tinkerpop.gremlin.structure.io.binary.TypeSerializerRegistry;
import org.janusgraph.core.attribute.Geoshape;
import org.janusgraph.graphdb.tinkerpop.JanusGraphIoRegistry;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

import static org.apache.tinkerpop.gremlin.driver.ser.AbstractMessageSerializer.TOKEN_IO_REGISTRIES;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class GeoshapeGraphBinarySerializerTest {
    private static final ByteBufAllocator allocator = ByteBufAllocator.DEFAULT;
    private static final NettyBufferFactory bufferFactory = new NettyBufferFactory();

    private static Stream<Geoshape> geoshapeProvider() {
        return Stream.of(
            Geoshape.point(37.97, 23.72),
            Geoshape.circle(37.97, 23.72, 10.0),
            Geoshape.box(37.97, 23.72, 38.97, 24.72)
        );
    }

    @ParameterizedTest
    @MethodSource("geoshapeProvider")
    public void shouldCustomSerialization(Geoshape geoshape) throws IOException {
        final GraphBinaryMessageSerializerV1 serializer = new GraphBinaryMessageSerializerV1(
            TypeSerializerRegistry.build().addCustomType(Geoshape.class, new GeoshapeGraphBinarySerializer()).create());

        assertGeoshape(serializer, geoshape);
    }

    @ParameterizedTest
    @MethodSource("geoshapeProvider")
    public void shouldSerializeViaIoRegistry(Geoshape geoshape) throws IOException {
        final GraphBinaryMessageSerializerV1 serializer = new GraphBinaryMessageSerializerV1();
        final Map<String, Object> config = new HashMap<>();
        config.put(TOKEN_IO_REGISTRIES, Collections.singletonList(JanusGraphIoRegistry.class.getName()));
        serializer.configure(config, Collections.emptyMap());

        assertGeoshape(serializer, geoshape);
    }

    @ParameterizedTest
    @MethodSource("geoshapeProvider")
    public void readValueAndWriteValueShouldBeSymmetric(Geoshape geoshape) throws IOException {
        final TypeSerializerRegistry registry = TypeSerializerRegistry.build()
            .addCustomType(Geoshape.class, new GeoshapeGraphBinarySerializer()).create();
        final GraphBinaryReader reader = new GraphBinaryReader(registry);
        final GraphBinaryWriter writer = new GraphBinaryWriter(registry);

        for (boolean nullable : new boolean[]{true, false}) {
            final Buffer buffer = bufferFactory.create(allocator.buffer());
            writer.writeValue(geoshape, buffer, nullable);
            final Geoshape actual = reader.readValue(buffer, Geoshape.class, nullable);

            assertEquals(actual, geoshape);
            buffer.release();
        }
    }

    private void assertGeoshape(final GraphBinaryMessageSerializerV1 serializer, final Geoshape geoshape) throws IOException {
        final ByteBuf serialized = serializer.serializeResponseAsBinary(
            ResponseMessage.build(UUID.randomUUID()).result(geoshape).create(), allocator);

        final ResponseMessage deserialized = serializer.deserializeResponse(serialized);

        final Geoshape actual = (Geoshape) deserialized.getResult().getData();
        assertEquals(actual, geoshape);
    }
}
