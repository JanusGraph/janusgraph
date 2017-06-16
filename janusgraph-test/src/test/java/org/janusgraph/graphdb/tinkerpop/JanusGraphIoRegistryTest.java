package org.janusgraph.graphdb.tinkerpop;

import static org.apache.tinkerpop.gremlin.process.traversal.P.between;
import static org.apache.tinkerpop.gremlin.process.traversal.P.inside;
import static org.apache.tinkerpop.gremlin.process.traversal.P.within;
import static org.apache.tinkerpop.gremlin.process.traversal.P.without;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.has;
import static org.junit.Assert.assertEquals;

import java.lang.reflect.Modifier;
import java.util.stream.Stream;

import org.apache.tinkerpop.gremlin.driver.MessageSerializer;
import org.apache.tinkerpop.gremlin.driver.Tokens;
import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;
import org.apache.tinkerpop.gremlin.driver.ser.GryoMessageSerializerV1d0;
import org.apache.tinkerpop.gremlin.driver.ser.SerializationException;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoMapper;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoMapper.Builder;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.janusgraph.core.attribute.Geo;
import org.janusgraph.core.attribute.Geoshape;
import org.janusgraph.core.attribute.Text;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;

public class JanusGraphIoRegistryTest {

    private final Logger log = LoggerFactory.getLogger(JanusGraphIoRegistryTest.class);

    private static ByteBufAllocator allocator = UnpooledByteBufAllocator.DEFAULT;

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
                    return true;
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
                g.V().has("name", Text.textContains("neptune")), g.V().has("name", Text.textContainsPrefix("nep")),
                g.V().has("name", Text.textContainsRegex("nep.*")), g.V().has("name", Text.textPrefix("n")),
                g.V().has("name", Text.textRegex(".*n.*"))

        };

        serializationTest(traversals);

    }

    @Test
    public void testGeoshapAsGryo() throws SerializationException {
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
        Builder mapper = GryoMapper.build().addRegistry(JanusGraphIoRegistry.getInstance());
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
}
