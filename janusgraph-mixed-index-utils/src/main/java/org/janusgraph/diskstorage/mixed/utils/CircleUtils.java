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

/**
 * Utility class for storing different helpful re-usable spatial functions. <br>
 * The implementation of this class is mostly taken from
 * <a href="https://github.com/elastic/elasticsearch/blob/8.6/libs/geo/src/main/java/org/elasticsearch/geometry/utils/CircleUtils.java">CircleUtils.java</a>
 * class available in `elasticsearch-geo 8.6.0` library. <br>
 * The main different is that this util class doesn't use ElasticSearch internal Geometry data structures.
 * Another difference is that this class reuses `SloppyMath.haversinMeters` from Lucene instead of relying on slowHaversin implementation.
 */
public class CircleUtils {

    static final int CIRCLE_TO_POLYGON_MINIMUM_NUMBER_OF_SIDES = 4;
    static final int CIRCLE_TO_POLYGON_MAXIMUM_NUMBER_OF_SIDES = 1000;

    private CircleUtils() {}

    /**
     * Makes an n-gon, centered at the provided circle's center, and each vertex approximately
     * `circleRadius` away from the center.
     *
     * It throws an IllegalArgumentException if the circle contains a pole.
     *
     * This does not split the polygon across the date-line.
     *
     * Adapted from from org.apache.lucene.tests.geo.GeoTestUtil
     *
     * @return matrix containing 2 arrays. Array with index 0 are Longitude coordinates and array with index 1 are Latitude coordinates.
     */
    public static double[][] createRegularGeoShapePolygon(double centerCenterLat, double circleCenterLon, double circleRadiusMeters, int gons) {
        if (SloppyMath.haversinMeters(centerCenterLat, circleCenterLon, 90, 0) < circleRadiusMeters) {
            throw new IllegalArgumentException(
                "circle [lat: " + centerCenterLat + " lon: "+circleCenterLon+" radius: "+circleRadiusMeters+"] contains the north pole. It cannot be translated to a polygon"
            );
        }
        if (SloppyMath.haversinMeters(centerCenterLat, circleCenterLon, -90, 0) < circleRadiusMeters) {
            throw new IllegalArgumentException(
                "circle [lat: " + centerCenterLat + " lon: "+circleCenterLon+" radius: "+circleRadiusMeters+"] contains the south pole. It cannot be translated to a polygon"
            );
        }

        double[][] result = new double[2][];
        result[0] = new double[gons + 1];
        result[1] = new double[gons + 1];
        for (int i = 0; i < gons; i++) {
            // make sure we do not start at angle 0 or we have issues at the poles
            double angle = i * (360.0 / gons);
            double x = Math.cos(Math.toRadians(angle));
            double y = Math.sin(Math.toRadians(angle));
            double factor = 2.0;
            double step = 1.0;
            int last = 0;

            // Iterate out along one spoke until we hone in on the point that's nearly exactly radiusMeters from the center:
            while (true) {
                double lat = centerCenterLat + y * factor;
                double lon = circleCenterLon + x * factor;
                double distanceMeters = SloppyMath.haversinMeters(centerCenterLat, circleCenterLon, lat, lon);

                if (Math.abs(distanceMeters - circleRadiusMeters) < 0.1) {
                    // Within 10 cm: close enough!
                    // lon/lat are left de-normalized so that indexing can properly detect dateline crossing.
                    result[0][i] = lon;
                    result[1][i] = lat;
                    break;
                }

                if (distanceMeters > circleRadiusMeters) {
                    // too big
                    factor -= step;
                    if (last == 1) {
                        step /= 2.0;
                    }
                    last = -1;
                } else if (distanceMeters < circleRadiusMeters) {
                    // too small
                    factor += step;
                    if (last == -1) {
                        step /= 2.0;
                    }
                    last = 1;
                }
            }
        }

        // close poly
        result[0][gons] = result[0][0];
        result[1][gons] = result[1][0];
        return result;
    }

    /**
     * Makes an n-gon, centered at the provided circle's center. This assumes
     * distance measured in cartesian geometry.
     *
     * @return matrix containing 2 arrays. Array with index 0 are X coordinates and array with index 1 are Y coordinates.
     **/
    public static double[][] createRegularShapePolygon(double circleCenterX, double circleCenterY, double circleRadius, int gons) {
        double[][] result = new double[2][];
        result[0] = new double[gons + 1];
        result[1] = new double[gons + 1];
        for (int i = 0; i < gons; i++) {
            double angle = i * (360.0 / gons);
            double x = circleRadius * Math.cos(Math.toRadians(angle));
            double y = circleRadius * Math.sin(Math.toRadians(angle));

            result[0][i] = x + circleCenterX;
            result[1][i] = y + circleCenterY;
        }
        // close poly
        result[0][gons] = result[0][0];
        result[1][gons] = result[1][0];
        return result;
    }

    public static int circleToPolygonNumSides(double radiusMeters, double errorDistanceMeters) {
        int val = (int) Math.ceil(2 * Math.PI / Math.acos(1 - errorDistanceMeters / radiusMeters));
        return Math.min(CIRCLE_TO_POLYGON_MAXIMUM_NUMBER_OF_SIDES, Math.max(CIRCLE_TO_POLYGON_MINIMUM_NUMBER_OF_SIDES, val));
    }

}
