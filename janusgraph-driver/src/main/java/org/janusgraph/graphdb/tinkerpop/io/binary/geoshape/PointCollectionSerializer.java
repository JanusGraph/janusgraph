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

import org.apache.tinkerpop.gremlin.structure.io.Buffer;
import org.janusgraph.core.attribute.Geoshape;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.spatial4j.shape.Shape;
import org.locationtech.spatial4j.shape.jts.JtsGeometry;

import java.util.ArrayList;
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
