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

package org.janusgraph.graphdb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.function.Function;

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.io.GraphReader;
import org.apache.tinkerpop.gremlin.structure.io.GraphWriter;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONMapper;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONReader;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONVersion;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONWriter;
import org.apache.tinkerpop.gremlin.structure.io.graphson.TypeInfo;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.attribute.Geoshape;
import org.janusgraph.core.attribute.JtsGeoshapeHelper;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.example.GraphOfTheGodsFactory;
import org.janusgraph.graphdb.tinkerpop.JanusGraphIoRegistry;
import org.janusgraph.graphdb.tinkerpop.JanusGraphIoRegistryV1d0;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.Polygon;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Tests JanusGraph specific serialization classes not covered by the TinkerPop suite.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(Parameterized.class)
public abstract class JanusGraphIoTest extends JanusGraphBaseTest {

    private static final GeometryFactory GF = new GeometryFactory();

    private static final JtsGeoshapeHelper HELPER = new JtsGeoshapeHelper();

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<Object[]> data() {

        final GraphSONMapper v1mapper = GraphSONMapper.build().version(GraphSONVersion.V1_0).typeInfo(TypeInfo.PARTIAL_TYPES).addRegistry(JanusGraphIoRegistryV1d0.getInstance()).create();
        final GraphSONMapper v2mapper = GraphSONMapper.build().version(GraphSONVersion.V2_0).typeInfo(TypeInfo.PARTIAL_TYPES).addRegistry(JanusGraphIoRegistry.getInstance()).create();
        final GraphSONMapper v3mapper = GraphSONMapper.build().version(GraphSONVersion.V3_0).typeInfo(TypeInfo.PARTIAL_TYPES).addRegistry(JanusGraphIoRegistry.getInstance()).create();

        return Arrays.asList(new Object[][]{
            {"graphson-v1-embedded",
                (Function<Graph, GraphReader>) g -> GraphSONReader.build().mapper(v1mapper).create(),
                (Function<Graph, GraphWriter>) g -> GraphSONWriter.build().mapper(v1mapper).create()},
            {"graphson-v2-embedded",
                (Function<Graph, GraphReader>) g -> GraphSONReader.build().mapper(v2mapper).create(),
                (Function<Graph, GraphWriter>) g -> GraphSONWriter.build().mapper(v2mapper).create()},
            {"graphson-v3",
                (Function<Graph, GraphReader>) g -> GraphSONReader.build().mapper(v3mapper).create(),
                (Function<Graph, GraphWriter>) g -> GraphSONWriter.build().mapper(v3mapper).create()},
            {"gryo",
                (Function<Graph, GraphReader>) g -> g.io(IoCore.gryo()).reader().mapper(g.io(IoCore.gryo()).mapper().create()).create(),
                (Function<Graph, GraphWriter>) g -> g.io(IoCore.gryo()).writer().mapper(g.io(IoCore.gryo()).mapper().create()).create()}
        });
    }

    @Parameterized.Parameter()
    public String ioType;

    @Parameterized.Parameter(value = 1)
    public Function<Graph, GraphReader> readerMaker;

    @Parameterized.Parameter(value = 2)
    public Function<Graph, GraphWriter> writerMaker;

    @Before
    public void setup() {
        GraphOfTheGodsFactory.loadWithoutMixedIndex(graph, true);
        JanusGraphManagement management = graph.openManagement();
        management.makePropertyKey("shape").dataType(Geoshape.class).make();
        management.commit();
    }

    @Test
    public void testSerialization() throws Exception {
        testSerialization(null);
        testSerialization(makeLine);
        testSerialization(makePoly);
        testSerialization(makeMultiPoint);
        testSerialization(makeMultiLine);
        testSerialization(makeMultiPolygon);
    }

    private void testSerialization(Function<Geoshape,Geoshape> makeGeoshape) throws Exception {
        if (makeGeoshape != null) {
            addGeoshape(makeGeoshape);
        }
        GraphWriter writer = writerMaker.apply(graph);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        writer.writeGraph(outputStream, graph);

        clearGraph(config);
        open(config);

        GraphReader reader = readerMaker.apply(graph);
        ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
        reader.readGraph(inputStream, graph);

        JanusGraphIndexTest.assertGraphOfTheGods(graph);
        if (makeGeoshape != null) {
            assertGeoshape(makeGeoshape);
        }
    }

    private void addGeoshape(Function<Geoshape,Geoshape> makeGeoshape) {
        JanusGraphTransaction tx = graph.newTransaction();
        graph.traversal().E().has("place").toList().forEach(e-> {
            Geoshape place = (Geoshape) e.property("place").value();
            e.property("shape", makeGeoshape.apply(place));
        });
        tx.commit();
    }

    private void assertGeoshape(Function<Geoshape,Geoshape> makeGeoshape) {
        graph.traversal().E().has("place").toList().forEach(e-> {
            assertTrue(e.property("shape").isPresent());
            Geoshape place = (Geoshape) e.property("place").value();
            Geoshape expected = makeGeoshape.apply(place);
            Geoshape actual = (Geoshape) e.property("shape").value();
            assertEquals(expected, actual);
        });
    }

    private static final Function<Geoshape,Geoshape> makePoly = place -> {
        double x = Math.floor(place.getPoint().getLongitude());
        double y = Math.floor(place.getPoint().getLatitude());
        return Geoshape.polygon(Arrays.asList(new double[][] {{x,y},{x,y+1},{x+1,y+1},{x+1,y},{x,y},{x,y}}));
    };

    private static final Function<Geoshape,Geoshape> makeLine = place -> {
        double x = Math.floor(place.getPoint().getLongitude());
        double y = Math.floor(place.getPoint().getLatitude());
        return Geoshape.line(Arrays.asList(new double[][] {{x,y},{x,y+1},{x+1,y+1},{x+1,y}}));
    };

    private static final Function<Geoshape,Geoshape> makeMultiPoint = place -> {
        double x = Math.floor(place.getPoint().getLongitude());
        double y = Math.floor(place.getPoint().getLatitude());
        return HELPER.geoshape(GF.createMultiPoint(new Coordinate[] {new Coordinate(x,y), new Coordinate(x+1,y+1)}));
    };

    private static final Function<Geoshape,Geoshape> makeMultiLine = place -> {
        double x = Math.floor(place.getPoint().getLongitude());
        double y = Math.floor(place.getPoint().getLatitude());
        return HELPER.geoshape(GF.createMultiLineString(new LineString[] {
                GF.createLineString(new Coordinate[] {new Coordinate(x,y), new Coordinate(x+1,y+1)}),
                GF.createLineString(new Coordinate[] {new Coordinate(x-1,y-1), new Coordinate(x,y)})}));
    };

    private static final Function<Geoshape,Geoshape> makeMultiPolygon = place -> {
        double x = Math.floor(place.getPoint().getLongitude());
        double y = Math.floor(place.getPoint().getLatitude());
        return HELPER.geoshape(GF.createMultiPolygon(new Polygon[] {
                GF.createPolygon(new Coordinate[] {new Coordinate(x,y), new Coordinate(x+1,y), new Coordinate(x+1,y+1), new Coordinate(x,y)}),
                GF.createPolygon(new Coordinate[] {new Coordinate(x+2,y+2), new Coordinate(x+2,y+3), new Coordinate(x+3,y+3), new Coordinate(x+2,y+2)})}));
    };

}
