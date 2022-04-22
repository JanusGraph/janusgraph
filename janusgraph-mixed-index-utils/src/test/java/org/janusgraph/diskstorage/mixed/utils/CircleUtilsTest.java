// Copyright 2023 JanusGraph Authors
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

package org.janusgraph.diskstorage.mixed.utils;

import org.apache.lucene.util.SloppyMath;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ThreadLocalRandom;

import static org.hamcrest.Matchers.closeTo;

/**
 * This test class implementation is mostly take from
 * <a href="https://github.com/elastic/elasticsearch/blob/8.6/libs/geo/src/test/java/org/elasticsearch/geometry/utils/CircleUtilsTests.java">CircleUtilsTests.java</a>
 */
public class CircleUtilsTest {

    @Test
    public void testCreateRegularGeoShapePolygon() {
        double lat = ThreadLocalRandom.current().nextDouble(-89, 89);
        double lon = ThreadLocalRandom.current().nextDouble(-179, 179);
        double radius = ThreadLocalRandom.current().nextDouble(10, 10000);
        doRegularGeoShapePolygon(lat, lon, radius);
    }

    @Test
    public void testCircleContainsNorthPole() {
        Exception exception = Assertions.assertThrows(Exception.class, () -> {
            doRegularGeoShapePolygon(90, 179, 100);
        });
        Assertions.assertTrue(exception.getMessage().contains("contains the north pole"));
    }

    @Test
    public void testCircleContainsSouthPole() {
        Exception exception = Assertions.assertThrows(Exception.class, () -> {
            doRegularGeoShapePolygon(-90, 179, 100);
        });
        Assertions.assertTrue(exception.getMessage().contains("contains the south pole"));
    }

    private void doRegularGeoShapePolygon(double lat, double lon, double radius) {
        int numSides = ThreadLocalRandom.current().nextInt(4, 1000);
        double[][] polygon = CircleUtils.createRegularGeoShapePolygon(lat, lon, radius, numSides);
        int numPoints = polygon[0].length;

        // check there are numSides edges
        Assertions.assertEquals(numSides + 1, numPoints);

        // check that all the points are about a radius away from the center
        for (int i = 0; i < numPoints; i++) {
            double actualDistance = SloppyMath.haversinMeters(lat, lon, polygon[1][i], polygon[0][i]);
            Assertions.assertTrue(closeTo(radius, 0.1).matches(actualDistance));
        }
    }

    @Test
    public void testCreateRegularShapePolygon() {
        double x = ThreadLocalRandom.current().nextDouble(-20, 20);
        double y = ThreadLocalRandom.current().nextDouble(-20, 20);
        double radius = ThreadLocalRandom.current().nextDouble(10, 10000);
        int numSides = ThreadLocalRandom.current().nextInt(4, 1000);
        double[][] polygon = CircleUtils.createRegularShapePolygon(x,y,radius, numSides);
        int numPoints = polygon[0].length;

        // check there are numSides edges
        Assertions.assertEquals(numSides + 1, numPoints);

        // check that all the points are about a radius away from the center
        for (int i = 0; i < numPoints; i++) {
            double deltaX = x - polygon[0][i];
            double deltaY = y - polygon[1][i];
            double distance = Math.sqrt(deltaX * deltaX + deltaY * deltaY);
            Assertions.assertTrue(closeTo(radius, 0.0001).matches(distance));
        }
    }

    @Test
    public void testCircleToPolygonNumSides() {
        // 0.01, 6371000
        double randomErrorDistanceMeters = ThreadLocalRandom.current().nextDouble(0.01, 6371000);
        double[] errorDistancesMeters = new double[] {
            0.0000001,
            0.0001,
            0.001,
            0.01,
            0.02,
            0.03,
            0.1,
            0.2,
            1,
            1.1,
            1.2,
            2,
            2.5,
            3,
            3.7,
            10,
            100,
            1000,
            10000,
            100000,
            1000000,
            5000000.12345,
            6370999.999,
            6371000,
            randomErrorDistanceMeters };

        for (double errorDistanceMeters : errorDistancesMeters) {
            // radius is same as error distance
            Assertions.assertEquals(4, CircleUtils.circleToPolygonNumSides(errorDistanceMeters, errorDistanceMeters));
            // radius is much smaller than error distance
            Assertions.assertEquals(4, CircleUtils.circleToPolygonNumSides(0, errorDistanceMeters));
            // radius is much larger than error distance
            double errorDistanceForPow = errorDistanceMeters;
            if (errorDistanceForPow < 1.12d) {
                errorDistanceForPow = 1.12;
            }
            Assertions.assertEquals(1000, CircleUtils.circleToPolygonNumSides(Math.pow(errorDistanceForPow, 100), errorDistanceForPow));
            // radius is 5 times longer than error distance
            Assertions.assertEquals(10, CircleUtils.circleToPolygonNumSides(5 * errorDistanceMeters, errorDistanceMeters));
        }
    }
}
