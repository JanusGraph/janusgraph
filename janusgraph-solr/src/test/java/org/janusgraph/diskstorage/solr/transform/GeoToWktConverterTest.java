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

package org.janusgraph.diskstorage.solr.transform;

import org.janusgraph.core.attribute.Geoshape;
import org.janusgraph.core.attribute.JtsGeoshapeHelper;
import org.janusgraph.diskstorage.BackendException;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.Polygon;

import java.text.ParseException;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author Jared Holmberg (jholmberg@bericotechnologies.com)
 */
public class GeoToWktConverterTest {

    private static final JtsGeoshapeHelper HELPER = new JtsGeoshapeHelper();

    /**

     * The GeoToWktConverter transforms the Geoshape's string value into a Well-Known Text
     * format understood by Solr.
     */
    @Test
    public void testConvertGeoshapePointToWktString() throws BackendException, ParseException {
        Geoshape p1 = Geoshape.point(35.4, 48.9); //no spaces, no negative values
        Geoshape p2 = Geoshape.point(-35.4,48.9); //negative longitude value
        Geoshape p3 = Geoshape.point(35.4, -48.9); //negative latitude value

        String wkt1 = "POINT(48.9 35.4)";
        assertEquals(p1.getPoint().getLongitude(), Geoshape.fromWkt(wkt1).getPoint().getLongitude(), 1e-5);
        assertEquals(p1.getPoint().getLatitude(), Geoshape.fromWkt(wkt1).getPoint().getLatitude(), 1e-5);

        String wkt2 = "POINT(48.9 -35.4)";
        assertEquals(p2.getPoint().getLongitude(), Geoshape.fromWkt(wkt2).getPoint().getLongitude(), 1e-5);
        assertEquals(p2.getPoint().getLatitude(), Geoshape.fromWkt(wkt2).getPoint().getLatitude(), 1e-5);

        String wkt3 = "POINT(-48.9 35.4)";
        assertEquals(p3.getPoint().getLongitude(), Geoshape.fromWkt(wkt3).getPoint().getLongitude(), 1e-5);
        assertEquals(p3.getPoint().getLatitude(), Geoshape.fromWkt(wkt3).getPoint().getLatitude(), 1e-5);
    }

    @Test
    public void testConvertGeoshapeLineToWktString() throws BackendException {
        Geoshape l1 = Geoshape.line(Arrays.asList(new double[][] {{48.9, 35.4}, {49.1, 35.6}}));

        String wkt1 = "LINESTRING (48.9 35.4, 49.1 35.6)";
        String actualWkt1 = GeoToWktConverter.convertToWktString(l1);
        assertEquals(wkt1, actualWkt1);
    }

    @Test
    public void testConvertGeoshapePolygonToWktString() throws BackendException {
        GeometryFactory gf = new GeometryFactory();
        Geoshape p1 = Geoshape.polygon(Arrays.asList(new double[][] {{35.4, 48.9}, {35.6, 48.9}, {35.6, 49.1}, {35.4, 49.1}, {35.4, 48.9}}));
        Geoshape p2 = HELPER.geoshape(gf.createPolygon(gf.createLinearRing(new Coordinate[] {
                new Coordinate(10,10), new Coordinate(20,10), new Coordinate(20,20), new Coordinate(10,20), new Coordinate(10,10)}),
                new LinearRing[] { gf.createLinearRing(new Coordinate[] {
                        new Coordinate(13,13), new Coordinate(17,13), new Coordinate(17,17), new Coordinate(13,17), new Coordinate(13,13)})}));

        String wkt1 = "POLYGON ((35.4 48.9, 35.6 48.9, 35.6 49.1, 35.4 49.1, 35.4 48.9))";
        String actualWkt1 = GeoToWktConverter.convertToWktString(p1);
        assertEquals(wkt1, actualWkt1);

        String wkt2 = "POLYGON ((10 10, 20 10, 20 20, 10 20, 10 10), (13 13, 17 13, 17 17, 13 17, 13 13))";
        String actualWkt2 = GeoToWktConverter.convertToWktString(p2);
        assertEquals(wkt2, actualWkt2);
    }

    @Test
    public void testConvertGeoshapeMultiPointToWktString() throws BackendException {
        GeometryFactory gf = new GeometryFactory();
        Geoshape g = HELPER.geoshape(gf.createMultiPoint(new Coordinate[] {new Coordinate(10,10), new Coordinate(20,20)}));

        String wkt1 = "MULTIPOINT ((10 10), (20 20))";
        String actualWkt1 = GeoToWktConverter.convertToWktString(g);
        assertEquals(wkt1, actualWkt1);
    }

    @Test
    public void testConvertGeoshapeMultiLineToWktString() throws BackendException {
        GeometryFactory gf = new GeometryFactory();
        Geoshape g = HELPER.geoshape(gf.createMultiLineString(new LineString[] {
                gf.createLineString(new Coordinate[] {new Coordinate(10,10), new Coordinate(20,20)}),
                gf.createLineString(new Coordinate[] {new Coordinate(30,30), new Coordinate(40,40)})}));

        String wkt1 = "MULTILINESTRING ((10 10, 20 20), (30 30, 40 40))";
        String actualWkt1 = GeoToWktConverter.convertToWktString(g);
        assertEquals(wkt1, actualWkt1);
    }

    @Test
    public void testConvertGeoshapeMultiPolygonToWktString() throws BackendException {
        GeometryFactory gf = new GeometryFactory();
        Geoshape g = HELPER.geoshape(gf.createMultiPolygon(new Polygon[] {
                gf.createPolygon(new Coordinate[] {new Coordinate(0,0), new Coordinate(0,10), new Coordinate(10,10), new Coordinate(0,0)}),
                gf.createPolygon(new Coordinate[] {new Coordinate(20,20), new Coordinate(20,30), new Coordinate(30,30), new Coordinate(20,20)})}));

        String wkt1 = "MULTIPOLYGON (((0 0, 0 10, 10 10, 0 0)), ((20 20, 20 30, 30 30, 20 20)))";
        String actualWkt1 = GeoToWktConverter.convertToWktString(g);
        assertEquals(wkt1, actualWkt1);
    }
}
