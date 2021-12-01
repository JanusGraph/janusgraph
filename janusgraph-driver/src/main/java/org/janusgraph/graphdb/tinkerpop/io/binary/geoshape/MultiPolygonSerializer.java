// Copyright 2021 JanusGraph Authors
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
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryReader;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryWriter;
import org.janusgraph.core.attribute.Geoshape;
import org.janusgraph.graphdb.tinkerpop.io.binary.GeoshapeGraphBinaryConstants;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.spatial4j.shape.Shape;
import org.locationtech.spatial4j.shape.ShapeFactory;
import org.locationtech.spatial4j.shape.jts.JtsGeometry;

import java.util.List;

public class MultiPolygonSerializer extends GeoshapeTypeSerializer {
    private static final PointCollectionSerializer pointCollectionSerializer = new PointCollectionSerializer();

    public MultiPolygonSerializer() { super(GeoshapeGraphBinaryConstants.GEOSHAPE_MULTI_POLYGON_TYPE_CODE); }

    @Override
    public Geoshape readNonNullableGeoshapeValue(final Buffer buffer, final GraphBinaryReader context) {
        final int nrPolygons = buffer.readInt();
        final ShapeFactory.MultiPolygonBuilder multiPolygonBuilder = Geoshape.getShapeFactory().multiPolygon();
        for (int i = 0; i < nrPolygons; i++) {
            final List<double[]> polygonPoints = pointCollectionSerializer.readPoints(buffer);
            final ShapeFactory.PolygonBuilder polygon = Geoshape.getShapeFactory().polygon();
            for (double[] point : polygonPoints) {
                polygon.pointXY(point[0], point[1]);
            }
            multiPolygonBuilder.add(polygon);
        }
        return Geoshape.geoshape(multiPolygonBuilder.build());
    }

    @Override
    public void writeNonNullableGeoshapeValue(final Geoshape geoshape, final Buffer buffer, final GraphBinaryWriter context) {
        final Shape shape = geoshape.getShape();
        final Geometry geom = ((JtsGeometry) shape).getGeom();

        final int nrPolygons = geom.getNumGeometries();
        buffer.writeInt(nrPolygons);
        for (int i = 0; i < nrPolygons; i++) {
            final Geometry polygon = geom.getGeometryN(i);
            pointCollectionSerializer.writePoints(polygon.getCoordinates(), buffer);
        }
    }
}
