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

package org.janusgraph.graphdb.tinkerpop.io.binary.geoshape;

import com.google.common.base.Preconditions;
import org.apache.tinkerpop.gremlin.structure.io.Buffer;
import org.janusgraph.core.attribute.Geoshape;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.spatial4j.shape.Shape;
import org.locationtech.spatial4j.shape.ShapeFactory;
import org.locationtech.spatial4j.shape.jts.JtsGeometry;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

class PointCollectionSerializer {

    public List<double[]> readPoints(final Buffer buffer) {
        final int length = buffer.readInt();
        final ArrayList points = new ArrayList(length);
        for (int i = 0; i < length; i++) {
            final double y = buffer.readDouble();
            final double x = buffer.readDouble();
            points.add(new double[] {x, y});
        }
        return points;
    }

    /**
     * Splits the flat coordinate list written by {@link #writePoints} back into its linear rings.
     * Each ring is written closed, i.e. it ends with a repeat of its first coordinate, so a polygon
     * with holes survives the round trip.
     */
    public List<List<double[]>> readRings(final Buffer buffer) {
        final List<double[]> points = readPoints(buffer);
        final List<List<double[]>> rings = new ArrayList<>();
        int ringStart = 0;
        while (ringStart < points.size()) {
            int ringEnd = ringStart + 1;
            while (ringEnd < points.size() && !Arrays.equals(points.get(ringStart), points.get(ringEnd))) {
                ringEnd++;
            }
            rings.add(points.subList(ringStart, Math.min(ringEnd + 1, points.size())));
            ringStart = ringEnd + 1;
        }
        return rings;
    }

    public ShapeFactory.PolygonBuilder polygonBuilder(final List<List<double[]>> rings) {
        Preconditions.checkArgument(!rings.isEmpty(), "No coordinate ring provided");
        final ShapeFactory.PolygonBuilder builder = Geoshape.getShapeFactory().polygon();
        for (double[] point : rings.get(0)) {
            checkCoordinate(point);
            builder.pointXY(point[0], point[1]);
        }
        for (int i = 1; i < rings.size(); i++) {
            final ShapeFactory.PolygonBuilder.HoleBuilder hole = builder.hole();
            for (double[] point : rings.get(i)) {
                checkCoordinate(point);
                hole.pointXY(point[0], point[1]);
            }
            hole.endHole();
        }
        return builder;
    }

    private void checkCoordinate(final double[] point) {
        Preconditions.checkArgument(point.length == 2 && Geoshape.isValidCoordinate(point[1], point[0]),
            "Invalid coordinate provided");
    }

    public void writePointCollectionGeoshape(final Geoshape geoshape, final Buffer buffer) {
        final Shape shape = geoshape.getShape();
        final Geometry geom = ((JtsGeometry) shape).getGeom();
        writePoints(geom.getCoordinates(), buffer);
    }

    public void writePoints(final Coordinate[] points, final Buffer buffer) {
        buffer.writeInt(points.length);
        for (Coordinate coordinate : points) {
            buffer.writeDouble(coordinate.getY());
            buffer.writeDouble(coordinate.getX());
        }
    }
}
