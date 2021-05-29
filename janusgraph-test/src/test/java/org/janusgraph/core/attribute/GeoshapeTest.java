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

package org.janusgraph.core.attribute;

import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;
import org.apache.tinkerpop.shaded.jackson.databind.module.SimpleModule;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.spatial4j.context.jts.JtsSpatialContext;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class GeoshapeTest {

    private static final JtsGeoshapeHelper HELPER = new JtsGeoshapeHelper();

    private static final GeometryFactory GF = new GeometryFactory();

    @Test
    public void testDistance() {
        Geoshape p1 = Geoshape.point(37.759, -122.536);
        Geoshape p2 = Geoshape.point(35.714, -105.938);

        double distance = 1496;
        assertEquals(distance,p1.getPoint().distance(p2.getPoint()),5.0);
    }

    @Test
    public void testIntersection() {
        for (int i=0;i<50;i++) {
            Geoshape point = Geoshape.point(i,i);
            Geoshape line = Geoshape.line(Arrays.asList(new double[][] {{i-1,i-1},{i,i},{i+1,i+1}}));
            Geoshape polygon = Geoshape.polygon(Arrays.asList(new double[][] {{i-1,i-1},{i,i-1},{i+1,0},{i+1,i+1},{i-1,i+1},{i-1,i-1}}));
            Geoshape circle = Geoshape.circle(i,i,point.getPoint().distance(Geoshape.point(i,i).getPoint())+10);
            assertTrue(circle.intersect(point));
            assertTrue(point.intersect(circle));
            assertTrue(circle.intersect(circle));
            assertTrue(polygon.intersect(circle));
            assertTrue(circle.intersect(polygon));
            assertTrue(line.intersect(circle));
            assertTrue(circle.intersect(line));
        }
    }

    @Test
    public void testEquality() {
        Geoshape c = Geoshape.circle(10.0,12.5,100);
        Geoshape b = Geoshape.box(20.0, 22.5, 40.5, 60.5);
        Geoshape l = Geoshape.line(Arrays.asList(new double[][] {{10.5,20.5},{10.5,22.5},{12.5,22.5}}));
        Geoshape p = Geoshape.polygon(Arrays.asList(new double[][] {{10.5,20.5},{8.0,21.75},{10.5,22.5},{11.75,25.0},{12.5,22.5},{15.0,21.0},{12.5,20.5},{11.75,18.0},{10.5,20.5}}));
        assertEquals(Geoshape.circle(10.0,12.5,100),c);
        assertEquals(Geoshape.box(20.0,22.5,40.5,60.5),b);
        assertEquals(Geoshape.line(Arrays.asList(new double[][] {{10.5,20.5},{10.5,22.5},{12.5,22.5}})),l);
        assertEquals(Geoshape.polygon(Arrays.asList(new double[][] {{10.5,20.5},{8.0,21.75},{10.5,22.5},{11.75,25.0},{12.5,22.5},{15.0,21.0},{12.5,20.5},{11.75,18.0},{10.5,20.5}})),p);
        assertEquals(Geoshape.circle(10.0,12.5,100).hashCode(),c.hashCode());
        assertEquals(Geoshape.box(20.0,22.5,40.5,60.5).hashCode(),b.hashCode());
        assertEquals(Geoshape.line(Arrays.asList(new double[][] {{10.5,20.5},{10.5,22.5},{12.5,22.5}})).hashCode(),l.hashCode());
        assertEquals(Geoshape.polygon(Arrays.asList(new double[][] {{10.5,20.5},{8.0,21.75},{10.5,22.5},{11.75,25.0},{12.5,22.5},{15.0,21.0},{12.5,20.5},{11.75,18.0},{10.5,20.5}})).hashCode(),p.hashCode());
        assertNotSame(c.hashCode(),b.hashCode());
        assertNotSame(c.hashCode(),l.hashCode());
        assertNotSame(c.hashCode(),p.hashCode());
        assertNotSame(b.hashCode(),l.hashCode());
        assertNotSame(b.hashCode(),p.hashCode());
        assertNotSame(l.hashCode(),p.hashCode());
        assertNotSame(c,b);
        assertNotSame(c,l);
        assertNotSame(c,p);
        assertNotSame(b,l);
        assertNotSame(b,p);
        assertNotSame(l,p);
    }


    @Test
    public void testParseCollection() {
        GeoshapeSerializer serializer = new GeoshapeSerializer();
        assertEquals(Geoshape.point(10, 20), serializer.convert(Arrays.asList(10, 20)));
        assertEquals(Geoshape.circle(10, 20, 30), serializer.convert(Arrays.asList(10, 20, 30)));
        assertEquals(Geoshape.box(10, 20, 30, 40), serializer.convert(Arrays.asList(10, 20, 30, 40)));
    }

    @Test
    public void testFailParseCollection() {
        assertThrows(IllegalArgumentException.class, () -> {
            GeoshapeSerializer serializer = new GeoshapeSerializer();
            serializer.convert(Arrays.asList(10, "Foo"));
        });
    }


    @Test
    public void testGeoJsonPoint() throws IOException {
        GeoshapeSerializer s = new GeoshapeSerializer();
        Map json = new ObjectMapper().readValue("{\n" +
                "  \"type\": \"Feature\",\n" +
                "  \"geometry\": {\n" +
                "    \"type\": \"Point\",\n" +
                "    \"coordinates\": [20.5, 10.5]\n" +
                "  },\n" +
                "  \"properties\": {\n" +
                "    \"name\": \"Dinagat Islands\"\n" +
                "  }\n" +
                "}", HashMap.class);
        assertEquals(Geoshape.point(10.5, 20.5), s.convert(json));
    }


    @Test
    public void testGeoJsonPointNotParsable() throws IOException {
        assertThrows(IllegalArgumentException.class, () -> {
            GeoshapeSerializer s = new GeoshapeSerializer();
            Map json = new ObjectMapper().readValue("{\n" +
                "  \"type\": \"Feature\",\n" +
                "  \"geometry\": {\n" +
                "    \"type\": \"Point\",\n" +
                "    \"coordinates\": [20.5, \"10.5\"]\n" +
                "  },\n" +
                "  \"properties\": {\n" +
                "    \"name\": \"Dinagat Islands\"\n" +
                "  }\n" +
                "}", HashMap.class);
            s.convert(json);
        });
    }


    @Test
    public void testGeoJsonCircle() throws IOException {
        GeoshapeSerializer s = new GeoshapeSerializer();
        Map json = new ObjectMapper().readValue("{\n" +
                "  \"type\": \"Feature\",\n" +
                "  \"geometry\": {\n" +
                "    \"type\": \"Circle\",\n" +
                "    \"radius\": 30.5, " +
                "    \"coordinates\": [20.5, 10.5]\n" +
                "  },\n" +
                "  \"properties\": {\n" +
                "    \"name\": \"Dinagat Islands\"\n" +
                "  }\n" +
                "}", HashMap.class);
        assertEquals(Geoshape.circle(10.5, 20.5, 30.5), s.convert(json));
    }

    @Test
    public void testGeoJsonCircleMissingRadius() throws IOException {
        assertThrows(IllegalArgumentException.class, () -> {
            GeoshapeSerializer s = new GeoshapeSerializer();
            Map json = new ObjectMapper().readValue("{\n" +
                "  \"type\": \"Feature\",\n" +
                "  \"geometry\": {\n" +
                "    \"type\": \"Circle\",\n" +
                "    \"coordinates\": [20.5, 10.5]\n" +
                "  },\n" +
                "  \"properties\": {\n" +
                "    \"name\": \"Dinagat Islands\"\n" +
                "  }\n" +
                "}", HashMap.class);
            s.convert(json);
        });
    }

    @Test
    public void testGeoJsonBox() throws IOException {
        GeoshapeSerializer s = new GeoshapeSerializer();
        Map json = new ObjectMapper().readValue("{\n" +
                "  \"type\": \"Feature\",\n" +
                "  \"geometry\": {\n" +
                "    \"type\": \"Polygon\",\n" +
                "    \"coordinates\": [[20.5, 10.5],[22.5, 10.5],[22.5, 12.5],[20.5, 12.5]]\n" +
                "  },\n" +
                "  \"properties\": {\n" +
                "    \"name\": \"Dinagat Islands\"\n" +
                "  }\n" +
                "}", HashMap.class);
        assertEquals(Geoshape.box(10.5, 20.5, 12.5, 22.5), s.convert(json));

        //Try the reverse order points
        json = new ObjectMapper().readValue("{\n" +
                "  \"type\": \"Feature\",\n" +
                "  \"geometry\": {\n" +
                "    \"type\": \"Polygon\",\n" +
                "    \"coordinates\": [[20.5, 12.5],[22.5, 12.5],[22.5, 10.5],[20.5, 10.5]]\n" +
                "  },\n" +
                "  \"properties\": {\n" +
                "    \"name\": \"Dinagat Islands\"\n" +
                "  }\n" +
                "}", HashMap.class);
        assertEquals(Geoshape.box(10.5, 20.5, 12.5, 22.5), s.convert(json));
    }

    @Test
    public void testGeoJsonInvalidBox1() throws IOException {
        assertThrows(IllegalArgumentException.class, () -> {
            GeoshapeSerializer s = new GeoshapeSerializer();
            Map json = new ObjectMapper().readValue("{\n" +
                "  \"type\": \"Feature\",\n" +
                "  \"geometry\": {\n" +
                "    \"type\": \"Polygon\",\n" +
                "    \"coordinates\": [[20.5, 12.5],[22.5, 12.5],[22.5, 10.5],[20.5, 10.6]]\n" +
                "  },\n" +
                "  \"properties\": {\n" +
                "    \"name\": \"Dinagat Islands\"\n" +
                "  }\n" +
                "}", HashMap.class);
            s.convert(json);
        });
    }

    @Test
    public void testGeoJsonInvalidBox2() throws IOException {
        assertThrows(IllegalArgumentException.class, () -> {
            GeoshapeSerializer s = new GeoshapeSerializer();
            Map json = new ObjectMapper().readValue("{\n" +
                "  \"type\": \"Feature\",\n" +
                "  \"geometry\": {\n" +
                "    \"type\": \"Polygon\",\n" +
                "    \"coordinates\": [[20.5, 10.5],[22.5, 10.5],[22.5, 12.5]]\n" +
                "  },\n" +
                "  \"properties\": {\n" +
                "    \"name\": \"Dinagat Islands\"\n" +
                "  }\n" +
                "}", HashMap.class);
            s.convert(json);
        });
    }

    @Test
    public void testGeoJsonLine() throws IOException {
        GeoshapeSerializer s = new GeoshapeSerializer();
        Map json = new ObjectMapper().readValue("{\n" +
                "  \"type\": \"Feature\",\n" +
                "  \"geometry\": {\n" +
                "    \"type\": \"LineString\",\n" +
                "    \"coordinates\": [[20.5, 10.5],[22.5, 10.5],[22.5, 12.5]]\n" +
                "  },\n" +
                "  \"properties\": {\n" +
                "    \"name\": \"Dinagat Islands\"\n" +
                "  }\n" +
                "}", HashMap.class);
         assertEquals(Geoshape.line(Arrays.asList(new double[][] {{20.5,10.5},{22.5,10.5},{22.5,12.5}})), s.convert(json));
    }

    @Test
    public void testGeoJsonPolygon() throws IOException {
        GeoshapeSerializer s = new GeoshapeSerializer();
        Map json = new ObjectMapper().readValue("{\n" +
                "  \"type\": \"Feature\",\n" +
                "  \"geometry\": {\n" +
                "    \"type\": \"Polygon\",\n" +
                "    \"coordinates\": [[[20.5,10.5],[21.75,8.0],[22.5,10.5],[25.0,11.75],[22.5,12.5],[21.0,15.0],[20.5,12.5],[18.0,11.75],[20.5,10.5]]]\n" +
                "  }" +
                "}", HashMap.class);
        assertEquals(Geoshape.polygon(Arrays.asList(new double[][] {{20.5,10.5},{21.75,8.0},{22.5,10.5},{25.0,11.75},{22.5,12.5},{21.0,15.0},{20.5,12.5},{18.0,11.75},{20.5,10.5}})), s.convert(json));
    }

    @Test
    public void testGeoJsonMultiPoint() throws IOException {
        GeoshapeSerializer s = new GeoshapeSerializer();
        Map json = new ObjectMapper().readValue("{\n" +
                "  \"type\": \"Feature\",\n" +
                "  \"geometry\": {\n" +
                "    \"type\": \"MultiPoint\",\n" +
                "    \"coordinates\": [[100.0, 0.0],[101.0, 1.0]]\n" +
                "  }" +
                "}", HashMap.class);
        assertEquals(HELPER.geoshape(GF.createMultiPoint(new Coordinate[] {new Coordinate(100,0), new Coordinate(101,1)})), s.convert(json));
    }

    @Test
    public void testGeoJsonMultiLineString() throws IOException {
        GeoshapeSerializer s = new GeoshapeSerializer();
        Map json = new ObjectMapper().readValue("{\n" +
                "  \"type\": \"Feature\",\n" +
                "  \"geometry\": {\n" +
                "    \"type\": \"MultiLineString\",\n" +
                "    \"coordinates\": [[[100.0,0.0],[101.0, 1.0]],[[102.0,2.0],[103.0,3.0]]]\n" +
                "  }" +
                "}", HashMap.class);
        assertEquals(HELPER.geoshape(GF.createMultiLineString(new LineString[] {
            GF.createLineString(new Coordinate[] {new Coordinate(100,0), new Coordinate(101,1)}),
            GF.createLineString(new Coordinate[] {new Coordinate(102,2), new Coordinate(103,3)})})), s.convert(json));
    }

    @Test
    public void testGeoJsonMultiPolygon() throws IOException {
        GeoshapeSerializer s = new GeoshapeSerializer();
        Map json = new ObjectMapper().readValue("{\n" +
                "  \"type\": \"Feature\",\n" +
                "  \"geometry\": {\n" +
                "    \"type\": \"MultiPolygon\",\n" +
                "    \"coordinates\": [[[[102.0,2.0],[103.0,2.0],[103.0,3.0],[102.0,3.0],[102.0,2.0]]]," +
                "[[[100.0,0.0],[101.0,0.0],[101.0,1.0],[100.0,1.0],[100.0,0.0]],[[100.2,0.2],[100.8,0.2],[100.8,0.8],[100.2,0.8],[100.2,0.2]]]]\n" +
                "  }" +
                "}", HashMap.class);
        assertEquals(HELPER.geoshape(GF.createMultiPolygon(new Polygon[] {
            GF.createPolygon(new Coordinate[] {new Coordinate(102,2), new Coordinate(103,2), new Coordinate(103,3), new Coordinate(102,3), new Coordinate(102,2)}),
            GF.createPolygon(GF.createLinearRing(new Coordinate[] {new Coordinate(100,0), new Coordinate(101,0), new Coordinate(101,1), new Coordinate(100,1), new Coordinate(100,0)}),
                new LinearRing[] { GF.createLinearRing(new Coordinate[] {new Coordinate(100.2,0.2), new Coordinate(100.8,0.2), new Coordinate(100.8,0.8), new Coordinate(100.2,0.8), new Coordinate(100.2,0.2)
                })})})), s.convert(json));
    }

    @Test
    public void testGeoJsonGeometry() throws IOException {
        GeoshapeSerializer s = new GeoshapeSerializer();
        Map json = new ObjectMapper().readValue("{\n" +
                "    \"type\": \"Point\",\n" +
                "    \"coordinates\": [20.5, 10.5]\n" +
                "}", HashMap.class);
        assertEquals(Geoshape.point(10.5, 20.5), s.convert(json));
    }

    @Test
    public void testGeoJsonSerialization() throws IOException {
        SimpleModule module = new SimpleModule();
        module.addSerializer(new Geoshape.GeoshapeGsonSerializerV2d0());
        final ObjectMapper om = new ObjectMapper();
        om.registerModule(module);
        JtsSpatialContext context = (JtsSpatialContext) Geoshape.getSpatialContext();
        assertEquals("{\"type\":\"Point\",\"coordinates\":[20.5,10.5]}", om.writeValueAsString(Geoshape.point(10.5, 20.5)));
        assertEquals("{\"type\":\"Polygon\",\"coordinates\":[[[20.5,10.5],[20.5,12.5],[22.5,12.5],[22.5,10.5],[20.5,10.5]]]}", om.writeValueAsString(Geoshape.box(10.5, 20.5, 12.5, 22.5)));
        assertEquals("{\"type\":\"Circle\",\"coordinates\":[20.5,10.5],\"radius\":30.5,\"properties\":{\"radius_units\":\"km\"}}", om.writeValueAsString(Geoshape.circle(10.5, 20.5, 30.5)));
        assertEquals("{\"type\":\"LineString\",\"coordinates\":[[20.5,10.5],[22.5,10.5],[22.5,12.5]]}", om.writeValueAsString(Geoshape.line(Arrays.asList(new double[][] {{20.5,10.5},{22.5,10.5},{22.5,12.5}}))));
        assertEquals("{\"type\":\"Polygon\",\"coordinates\":[[[20.5,10.5],[21.75,8],[22.5,10.5],[25,11.75],[22.5,12.5],[21,15],[20.5,12.5],[18,11.75],[20.5,10.5]]]}",
            om.writeValueAsString(Geoshape.polygon(Arrays.asList(new double[][] {{20.5,10.5},{21.75,8},{22.5,10.5},{25,11.75},{22.5,12.5},{21,15},{20.5,12.5},{18,11.75},{20.5,10.5}}))));
        assertEquals("{\"type\":\"MultiPoint\",\"coordinates\":[[100,0],[101,1]]}",
            om.writeValueAsString(Geoshape.geoshape(context.getShapeFactory().makeShapeFromGeometry(GF.createMultiPoint(new Coordinate[] {new Coordinate(100,0), new Coordinate(101,1)})))));
        assertEquals("{\"type\":\"MultiLineString\",\"coordinates\":[[[100,0],[101,1]],[[102,2],[103,3]]]}",
            om.writeValueAsString(Geoshape.geoshape(context.getShapeFactory().makeShapeFromGeometry(GF.createMultiLineString(new LineString[] {
                GF.createLineString(new Coordinate[] {new Coordinate(100,0), new Coordinate(101,1)}),
                GF.createLineString(new Coordinate[] {new Coordinate(102,2), new Coordinate(103,3)})})))));
        assertEquals("{\"type\":\"MultiPolygon\",\"coordinates\":[[[[102,2],[103,2],[103,3],[102,3],[102,2]]],[[[100,0],[101,0],[101,1],[100,1],[100,0]],[[100.2,0.2],[100.8,0.2],[100.8,0.8],[100.2,0.8],[100.2,0.2]]]]}",
            om.writeValueAsString(Geoshape.geoshape(context.getShapeFactory().makeShapeFromGeometry(GF.createMultiPolygon(new Polygon[] {
                GF.createPolygon(new Coordinate[] {new Coordinate(102,2), new Coordinate(103,2), new Coordinate(103,3), new Coordinate(102,3), new Coordinate(102,2)}),
                GF.createPolygon(GF.createLinearRing(new Coordinate[] {new Coordinate(100,0), new Coordinate(101,0), new Coordinate(101,1), new Coordinate(100,1), new Coordinate(100,0)}),
                    new LinearRing[] { GF.createLinearRing(new Coordinate[] {new Coordinate(100.2,0.2), new Coordinate(100.8,0.2),
                        new Coordinate(100.8,0.8), new Coordinate(100.2,0.8), new Coordinate(100.2,0.2)})})})))));
    }


}
