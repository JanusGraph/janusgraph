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
import org.janusgraph.core.attribute.Geo;
import org.janusgraph.core.attribute.Geoshape;
import org.janusgraph.core.attribute.Text;
import org.janusgraph.graphdb.tinkerpop.JanusGraphIoRegistry;
import org.janusgraph.graphdb.tinkerpop.io.JanusGraphP;
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

public class JanusGraphPGraphBinarySerializerTest {

    private static final ByteBufAllocator allocator = ByteBufAllocator.DEFAULT;
    private static final NettyBufferFactory bufferFactory = new NettyBufferFactory();

    private static Stream<JanusGraphP> janusGraphPProvider() {
        return Stream.of(
            Geo.geoIntersect(Geoshape.circle(37.97, 23.72, 50)),
            Geo.geoWithin(Geoshape.circle(37.97, 23.72, 50)),
            Geo.geoDisjoint(Geoshape.circle(37.97, 23.72, 50)),
            Geo.geoContains(Geoshape.point(37.97, 23.72)),
            Text.textContains("neptune"),
            Text.textContainsPrefix("nep"),
            Text.textContainsRegex("nep.*"),
            Text.textPrefix("n"),
            Text.textRegex(".*n.*"),
            Text.textContainsFuzzy("neptun"),
            Text.textFuzzy("nepitne")
        );
    }

    @ParameterizedTest
    @MethodSource("janusGraphPProvider")
    public void shouldCustomSerialization(final JanusGraphP predicate) throws IOException {
        final GraphBinaryMessageSerializerV1 serializer = new GraphBinaryMessageSerializerV1(
            TypeSerializerRegistry.build()
                .addCustomType(JanusGraphP.class, new JanusGraphPBinarySerializer())
                .addCustomType(Geoshape.class, new GeoshapeGraphBinarySerializer())
                .create());

        assertJanusGraphP(serializer, predicate);
    }

    @ParameterizedTest
    @MethodSource("janusGraphPProvider")
    public void shouldSerializePersonViaIoRegistry(final JanusGraphP predicate) throws IOException {
        final GraphBinaryMessageSerializerV1 serializer = new GraphBinaryMessageSerializerV1();
        final Map<String, Object> config = new HashMap<>();
        config.put(TOKEN_IO_REGISTRIES, Collections.singletonList(JanusGraphIoRegistry.class.getName()));
        serializer.configure(config, Collections.emptyMap());

        assertJanusGraphP(serializer, predicate);
    }

    @ParameterizedTest
    @MethodSource("janusGraphPProvider")
    public void readValueAndWriteValueShouldBeSymmetric(final JanusGraphP predicate) throws IOException {
        final TypeSerializerRegistry registry = TypeSerializerRegistry.build()
            .addCustomType(JanusGraphP.class, new JanusGraphPBinarySerializer())
            .addCustomType(Geoshape.class, new GeoshapeGraphBinarySerializer())
            .create();
        final GraphBinaryReader reader = new GraphBinaryReader(registry);
        final GraphBinaryWriter writer = new GraphBinaryWriter(registry);

        for (boolean nullable : new boolean[]{true, false}) {
            final Buffer buffer = bufferFactory.create(allocator.buffer());
            writer.writeValue(predicate, buffer, nullable);
            final JanusGraphP actual = reader.readValue(buffer, JanusGraphP.class, nullable);

            assertEquals(actual.toString(), predicate.toString());
            buffer.release();
        }
    }

    private void assertJanusGraphP(final GraphBinaryMessageSerializerV1 serializer, final JanusGraphP predicate) throws IOException {
        final ByteBuf serialized = serializer.serializeResponseAsBinary(
            ResponseMessage.build(UUID.randomUUID()).result(predicate).create(), allocator);

        final ResponseMessage deserialized = serializer.deserializeResponse(serialized);

        final JanusGraphP actual = (JanusGraphP) deserialized.getResult().getData();
        assertEquals(actual, predicate);
    }
}
