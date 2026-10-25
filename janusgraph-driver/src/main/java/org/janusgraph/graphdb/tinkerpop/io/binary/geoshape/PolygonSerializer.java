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

import java.util.List;

public class PolygonSerializer extends GeoshapeTypeSerializer {
    private static final PointCollectionSerializer pointCollectionSerializer = new PointCollectionSerializer();

    public PolygonSerializer() { super(GeoshapeGraphBinaryConstants.GEOSHAPE_POLYGON_TYPE_CODE); }

    @Override
    public Geoshape readNonNullableGeoshapeValue(final Buffer buffer, final GraphBinaryReader context) {
        final List<List<double[]>> rings = pointCollectionSerializer.readRings(buffer);
        if (rings.size() == 1) {
            return Geoshape.polygon(rings.get(0));
        }
        return Geoshape.geoshape(pointCollectionSerializer.polygonBuilder(rings).build());
    }

    @Override
    public void writeNonNullableGeoshapeValue(final Geoshape geoshape, final Buffer buffer, final GraphBinaryWriter context) {
        pointCollectionSerializer.writePointCollectionGeoshape(geoshape, buffer);
    }
}
