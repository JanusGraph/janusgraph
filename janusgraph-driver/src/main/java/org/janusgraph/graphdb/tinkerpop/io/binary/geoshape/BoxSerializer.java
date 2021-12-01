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
import org.locationtech.spatial4j.shape.Rectangle;
import org.locationtech.spatial4j.shape.Shape;

public class BoxSerializer extends GeoshapeTypeSerializer {

    public BoxSerializer() { super(GeoshapeGraphBinaryConstants.GEOSHAPE_BOX_TYPE_CODE); }

    @Override
    public Geoshape readNonNullableGeoshapeValue(final Buffer buffer, final GraphBinaryReader context) {
        final double minY = buffer.readDouble();
        final double minX = buffer.readDouble();
        final double maxY = buffer.readDouble();
        final double maxX = buffer.readDouble();
        return Geoshape.box(minY, minX, maxY, maxX);
    }

    @Override
    public void writeNonNullableGeoshapeValue(final Geoshape geoshape, final Buffer buffer, final GraphBinaryWriter context) {
        final Shape shape = geoshape.getShape();
        final Rectangle rect = (Rectangle) shape;
        buffer.writeDouble(rect.getMinY());
        buffer.writeDouble(rect.getMinX());
        buffer.writeDouble(rect.getMaxY());
        buffer.writeDouble(rect.getMaxX());
    }
}
