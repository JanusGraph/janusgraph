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
import org.locationtech.spatial4j.shape.Shape;
import org.locationtech.spatial4j.shape.ShapeCollection;
import org.locationtech.spatial4j.shape.ShapeFactory;

import java.io.IOException;
import java.util.List;

public class GeometryCollectionSerializer extends GeoshapeTypeSerializer {

    public GeometryCollectionSerializer() { super(GeoshapeGraphBinaryConstants.GEOSHAPE_GEOMETRY_COLLECTION_TYPE_CODE); }

    @Override
    public Geoshape readNonNullableGeoshapeValue(final Buffer buffer, final GraphBinaryReader context) throws IOException {
        final int nrShapes = buffer.readInt();
        final ShapeFactory.MultiShapeBuilder<Shape> geometryCollectionBuilder = Geoshape.getGeometryCollectionBuilder();
        for (int i = 0; i < nrShapes; i++) {
            final Geoshape shape = context.readValue(buffer, Geoshape.class, true);
            geometryCollectionBuilder.add(shape.getShape());
        }
        return Geoshape.geoshape(geometryCollectionBuilder.build());
    }

    @Override
    public void writeNonNullableGeoshapeValue(final Geoshape geoshape, final Buffer buffer, final GraphBinaryWriter context) throws IOException {
        final ShapeCollection shapeCollection = (ShapeCollection) geoshape.getShape();
        final List<Shape> shapes = shapeCollection.getShapes();
        buffer.writeInt(shapes.size());
        for (Shape shape : shapes) {
            final Geoshape geoshapeMember = Geoshape.geoshape(shape);
            context.writeValue(geoshapeMember, buffer, true);
        }
    }
}
