// Copyright 2019 JanusGraph Authors
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

package org.janusgraph.graphdb.tinkerpop;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.apache.tinkerpop.gremlin.driver.MessageSerializer;
import org.apache.tinkerpop.gremlin.driver.Tokens;
import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;
import org.apache.tinkerpop.gremlin.driver.ser.GraphSONMessageSerializerV3d0;
import org.apache.tinkerpop.gremlin.driver.ser.GryoMessageSerializerV1d0;
import org.apache.tinkerpop.gremlin.driver.ser.SerializationException;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONMapper;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONReader;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONVersion;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONWriter;
import org.apache.tinkerpop.gremlin.structure.io.graphson.TypeInfo;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoMapper;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoMapper.Builder;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.apache.tinkerpop.shaded.kryo.Kryo;
import org.apache.tinkerpop.shaded.kryo.io.Input;
import org.apache.tinkerpop.shaded.kryo.io.Output;
import org.janusgraph.core.attribute.Geo;
import org.janusgraph.core.attribute.Geoshape;
import org.janusgraph.core.attribute.Text;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.lang.reflect.Modifier;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static org.apache.tinkerpop.gremlin.driver.ser.AbstractMessageSerializer.TOKEN_IO_REGISTRIES;
import static org.apache.tinkerpop.gremlin.process.traversal.P.between;
import static org.apache.tinkerpop.gremlin.process.traversal.P.inside;
import static org.apache.tinkerpop.gremlin.process.traversal.P.within;
import static org.apache.tinkerpop.gremlin.process.traversal.P.without;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.has;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class JanusGraphIoRegistryTest {

    private final Logger log = LoggerFactory.getLogger(JanusGraphIoRegistryTest.class);

    private static final ByteBufAllocator allocator = UnpooledByteBufAllocator.DEFAULT;

    /**
     * This is necessary since we replace the default TinkerPop PSerializer
     * 
     * @throws Exception
     */
    @Test
    public void testTinkerPopPredicatesAsGryo() throws SerializationException {

        // Don't change this trivially. At the time of this writing (TinkerPop
        // 3.2.3), this is how many P predicate methods were defined. If this
        // fails, then JanusGraphPSerializer needs to be updated to add/remove
        // any TinkerPop predicates!
        assertEquals(15,
                Stream.of(P.class.getDeclaredMethods()).filter(m -> Modifier.isStatic(m.getModifiers())).filter(p -> {
                    log.debug("Predicate: {}", p);
                    return !p.isSynthetic();
                }).count());

        Graph graph = EmptyGraph.instance();
        GraphTraversalSource g = graph.traversal();

        // TinkerPop Predicates
        GraphTraversal[] traversals = { g.V().has("age", within(5000)), g.V().has("age", without(5000)),
                g.V().has("age", within(5000, 45)), g.V().has("age", inside(45, 5000)),
                g.V().and(has("age", between(45, 5000)), has("name", within("pluto"))),
                g.V().or(has("age", between(45, 5000)), has("name", within("pluto", "neptune"))) };

        serializationTest(traversals);
    }

    @Test
    public void testJanusGraphPredicatesAsGryo() throws SerializationException {

        Graph graph = EmptyGraph.instance();
        GraphTraversalSource g = graph.traversal();

        // Janus Graph Geo, Text Predicates
        GraphTraversal[] traversals = { g.E().has("place", Geo.geoIntersect(Geoshape.circle(37.97, 23.72, 50))),
                g.E().has("place", Geo.geoWithin(Geoshape.circle(37.97, 23.72, 50))),
                g.E().has("place", Geo.geoDisjoint(Geoshape.circle(37.97, 23.72, 50))),
                g.V().has("place", Geo.geoContains(Geoshape.point(37.97, 23.72))),
                g.V().has("name", Text.textContains("neptune")), g.V().has("name", Text.textContainsPrefix("nep")),
                g.V().has("name", Text.textContainsRegex("nep.*")), g.V().has("name", Text.textPrefix("n")),
                g.V().has("name", Text.textRegex(".*n.*")), g.V().has("name", Text.textContainsFuzzy("neptun")),
                g.V().has("name", Text.textFuzzy("nepitne")) };

        serializationTest(traversals);
    }

    @Test
    public void testGeoshapeAsGryo() throws SerializationException {
        Graph graph = EmptyGraph.instance();
        GraphTraversalSource g = graph.traversal();

        GraphTraversal[] traversals = { g.addV().property("loc", Geoshape.box(0.1d, 0.2d, 0.3d, 0.4d)),
                g.addV().property("loc", Geoshape.box(0.1f, 0.3f, 0.5f, 0.6f)),
                g.addV().property("loc", Geoshape.circle(0.1d, 0.3d, 0.3d)),
                g.addV().property("loc", Geoshape.circle(0.2f, 0.4f, 0.5f)),
                g.addV().property("loc", Geoshape.point(1.0d, 4.0d)),
                g.addV().property("loc", Geoshape.point(1.0f, 1.0f)) };

        serializationTest(traversals);
    }

    private void serializationTest(GraphTraversal[] traversals) throws SerializationException {
        Builder mapper = GryoMapper.build().addRegistry(JanusGraphIoRegistry.instance());
        MessageSerializer binarySerializer = new GryoMessageSerializerV1d0(mapper);

        for (GraphTraversal traversal : traversals) {
            Bytecode expectedBytecode = traversal.asAdmin().getBytecode();
            RequestMessage requestMessage = RequestMessage.build(Tokens.OPS_BYTECODE).processor("traversal")
                    .addArg(Tokens.ARGS_GREMLIN, expectedBytecode).create();

            ByteBuf bb = binarySerializer.serializeRequestAsBinary(requestMessage, allocator);
            final int mimeLen = bb.readByte();
            bb.readBytes(new byte[mimeLen]);
            RequestMessage deser = binarySerializer.deserializeRequest(bb);
            Bytecode result = (Bytecode) deser.getArgs().get(Tokens.ARGS_GREMLIN);
            assertEquals(expectedBytecode, result);
        }
    }

    @Test
    public void testLegacyGeoshapeAsGryo() {
        final Geoshape point = Geoshape.point(1.0d, 4.0d);

        Kryo kryo = new Kryo();
        ByteArrayOutputStream outStream = new ByteArrayOutputStream();
		Output output = new Output(outStream, 4096);
		output.writeLong(1);
		output.writeFloat((float) point.getPoint().getLatitude());
        output.writeFloat((float) point.getPoint().getLongitude());
        output.flush();

        Geoshape.GeoShapeGryoSerializer serializer = new Geoshape.GeoShapeGryoSerializer();
        Input input = new Input(new ByteArrayInputStream(outStream.toByteArray()), 4096);
        assertEquals(point, serializer.read(kryo, input, Geoshape.class));
    }

    @Test
    public void testTokenIoRegistyInConfig() throws SerializationException {
        final GraphSONMessageSerializerV3d0 serializer = new GraphSONMessageSerializerV3d0();
        final Map<String,Object> config = new HashMap<>();
        config.put(TOKEN_IO_REGISTRIES, Collections.singletonList(JanusGraphIoRegistry.class.getName()));
        serializer.configure(config, Collections.emptyMap());

        GraphTraversal traversal = EmptyGraph.instance().traversal().addV().property("loc", Geoshape.point(1.0f, 1.0f));
        Bytecode expectedBytecode = traversal.asAdmin().getBytecode();

        String serializedMessage = serializer.serializeRequestAsString(
            RequestMessage.build(Tokens.OPS_BYTECODE).processor("traversal")
            .addArg(Tokens.ARGS_GREMLIN, expectedBytecode).create());

        RequestMessage requestMessage1 = serializer.deserializeRequest(serializedMessage);
        Bytecode result = (Bytecode)requestMessage1.getArgs().get(Tokens.ARGS_GREMLIN);
        assertEquals(expectedBytecode, result);
    }

    @Test
    public void testOldFormatJanusGraphPredicatesAsGryo() throws SerializationException {
        Graph graph = EmptyGraph.instance();
        GraphTraversalSource g = graph.traversal();

        Bytecode oldBytecode = serializeByteCodeAfterDeserializeAsGryo(g.V().has("name", new P<>(Text.CONTAINS, "test")));
        Bytecode newBytecode = serializeByteCodeAfterDeserializeAsGryo(g.V().has("name", Text.textContains("test")));

        assertEquals(newBytecode, oldBytecode);
    }

    private Bytecode serializeByteCodeAfterDeserializeAsGryo(GraphTraversal traversal) throws SerializationException {
        Builder mapper = GryoMapper.build().addRegistry(JanusGraphIoRegistry.instance());
        MessageSerializer binarySerializer = new GryoMessageSerializerV1d0(mapper);
        Bytecode expectedBytecode = traversal.asAdmin().getBytecode();
        RequestMessage requestMessage = RequestMessage.build(Tokens.OPS_BYTECODE).processor("traversal")
            .addArg(Tokens.ARGS_GREMLIN, expectedBytecode).create();

        ByteBuf bb = binarySerializer.serializeRequestAsBinary(requestMessage, allocator);
        final int mimeLen = bb.readByte();
        bb.readBytes(new byte[mimeLen]);
        RequestMessage deser = binarySerializer.deserializeRequest(bb);
        return  (Bytecode) deser.getArgs().get(Tokens.ARGS_GREMLIN);
    }

    @Test
    public void testOldFormatJanusGraphPredicatesAsGraphSON() throws Exception {
        Graph graph = EmptyGraph.instance();
        GraphTraversalSource g = graph.traversal();

        Bytecode oldBytecode = serializeByteCodeAfterDeserializeAsGraphSON(g.V().has("name", new P<>(Text.CONTAINS, "test")));
        Bytecode newBytecode = serializeByteCodeAfterDeserializeAsGraphSON(g.V().has("name", Text.textContains("test")));

        assertEquals(newBytecode, oldBytecode);
    }

    private Bytecode serializeByteCodeAfterDeserializeAsGraphSON(GraphTraversal traversal) throws Exception {
        final GraphSONMapper mapper = GraphSONMapper.build().version(GraphSONVersion.V3_0)
            .typeInfo(TypeInfo.PARTIAL_TYPES).addRegistry(JanusGraphIoRegistry.instance()).create();
        final GraphSONWriter writer = GraphSONWriter.build().mapper(mapper).create();
        final GraphSONReader reader = GraphSONReader.build().mapper(mapper).create();
        Bytecode expectedBytecode = traversal.asAdmin().getBytecode();
        ByteArrayOutputStream serializationStream = new ByteArrayOutputStream();
        writer.writeObject(serializationStream, expectedBytecode);

        ByteArrayInputStream inputStream = new ByteArrayInputStream(serializationStream.toByteArray());

        return reader.readObject(inputStream, Bytecode.class);
    }
}
