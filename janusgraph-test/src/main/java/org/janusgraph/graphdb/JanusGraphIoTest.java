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

import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.attribute.Geoshape;
import org.janusgraph.core.attribute.JtsGeoshapeHelper;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.example.GraphOfTheGodsFactory;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.Polygon;

import org.apache.tinkerpop.gremlin.structure.io.GraphReader;
import org.apache.tinkerpop.gremlin.structure.io.GraphWriter;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONMapper;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.Arrays;
import java.util.function.Function;

/**
 * Tests JanusGraph specific serialization classes not covered by the TinkerPop suite.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class JanusGraphIoTest extends JanusGraphBaseTest {

    private static final GeometryFactory GF = new GeometryFactory();

    private static final JtsGeoshapeHelper HELPER = new JtsGeoshapeHelper();

    @Before
    public void setup() {
        GraphOfTheGodsFactory.loadWithoutMixedIndex(graph, true);
        JanusGraphManagement mgmt = graph.openManagement();
        mgmt.makePropertyKey("shape").dataType(Geoshape.class).make();
        mgmt.commit();
    }

    @Test
    public void testSerializationReadWriteAsGraphSONEmbedded() throws Exception {
        testSerializationReadWriteAsGraphSONEmbedded(null);
        testSerializationReadWriteAsGraphSONEmbedded(makeLine);
        testSerializationReadWriteAsGraphSONEmbedded(makePoly);
        testSerializationReadWriteAsGraphSONEmbedded(makeMultiPoint);
        testSerializationReadWriteAsGraphSONEmbedded(makeMultiLine);
        testSerializationReadWriteAsGraphSONEmbedded(makeMultiPolygon);
    }

    @Test
    public void testSerializationReadWriteAsGryo() throws Exception {
        testSerializationReadWriteAsGryo(null);
        testSerializationReadWriteAsGryo(makeLine);
        testSerializationReadWriteAsGryo(makePoly);
        testSerializationReadWriteAsGryo(makeMultiPoint);
        testSerializationReadWriteAsGryo(makeMultiLine);
        testSerializationReadWriteAsGryo(makeMultiPolygon);
    }

    public void testSerializationReadWriteAsGraphSONEmbedded(Function<Geoshape,Geoshape> makeGeoshape) throws Exception {
        if (makeGeoshape != null) {
            addGeoshape(makeGeoshape);
        }
        GraphSONMapper m = graph.io(IoCore.graphson()).mapper().embedTypes(true).create();
        GraphWriter writer = graph.io(IoCore.graphson()).writer().mapper(m).create();
        FileOutputStream fos = new FileOutputStream("/tmp/test.json");
        writer.writeGraph(fos, graph);

        clearGraph(config);
        open(config);

        GraphReader reader = graph.io(IoCore.graphson()).reader().mapper(m).create();
        FileInputStream fis = new FileInputStream("/tmp/test.json");
        reader.readGraph(fis, graph);

        JanusGraphIndexTest.assertGraphOfTheGods(graph);
        if (makeGeoshape != null) {
            assertGeoshape(makeGeoshape);
        }
    }

    private void testSerializationReadWriteAsGryo(Function<Geoshape,Geoshape> makeGeoshape) throws Exception {
        if (makeGeoshape != null) {
            addGeoshape(makeGeoshape);
        }
        graph.io(IoCore.gryo()).writeGraph("/tmp/test.kryo");

        clearGraph(config);
        open(config);

        graph.io(IoCore.gryo()).readGraph("/tmp/test.kryo");

        JanusGraphIndexTest.assertGraphOfTheGods(graph);
        if (makeGeoshape != null) {
            assertGeoshape(makeGeoshape);
        }
    }

    private void addGeoshape(Function<Geoshape,Geoshape> makeGeoshape) {
        JanusGraphTransaction tx = graph.newTransaction();
        graph.traversal().E().has("place").toList().stream().forEach(e-> {
            Geoshape place = (Geoshape) e.property("place").value();
            e.property("shape", makeGeoshape.apply(place));
        });
        tx.commit();
    }

    private void assertGeoshape(Function<Geoshape,Geoshape> makeGeoshape) {
        graph.traversal().E().has("place").toList().stream().forEach(e-> {
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
