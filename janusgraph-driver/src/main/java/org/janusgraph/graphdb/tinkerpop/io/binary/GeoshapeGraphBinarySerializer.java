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

package org.janusgraph.graphdb.tinkerpop.io.binary;

import org.apache.tinkerpop.gremlin.structure.io.Buffer;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryReader;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryWriter;
import org.apache.tinkerpop.gremlin.util.ser.SerializationException;
import org.janusgraph.core.attribute.Geoshape;
import org.janusgraph.graphdb.tinkerpop.io.binary.geoshape.BoxSerializer;
import org.janusgraph.graphdb.tinkerpop.io.binary.geoshape.CircleSerializer;
import org.janusgraph.graphdb.tinkerpop.io.binary.geoshape.GeometryCollectionSerializer;
import org.janusgraph.graphdb.tinkerpop.io.binary.geoshape.LineSerializer;
import org.janusgraph.graphdb.tinkerpop.io.binary.geoshape.MultiLineSerializer;
import org.janusgraph.graphdb.tinkerpop.io.binary.geoshape.MultiPointSerializer;
import org.janusgraph.graphdb.tinkerpop.io.binary.geoshape.MultiPolygonSerializer;
import org.janusgraph.graphdb.tinkerpop.io.binary.geoshape.PointSerializer;
import org.janusgraph.graphdb.tinkerpop.io.binary.geoshape.PolygonSerializer;
import org.janusgraph.graphdb.tinkerpop.io.binary.geoshape.GeoshapeTypeSerializer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class GeoshapeGraphBinarySerializer extends JanusGraphTypeSerializer<Geoshape> {

    private final Map<Integer, GeoshapeTypeSerializer> serializerByGeoshapeTypeCode = new HashMap<>();
    private final Map<Geoshape.Type, GeoshapeTypeSerializer> serializerByGeoshapeType = new HashMap<>();

    public GeoshapeGraphBinarySerializer() {
        super(GraphBinaryType.Geoshape);

        serializerByGeoshapeTypeCode.put(GeoshapeGraphBinaryConstants.GEOSHAPE_POINT_TYPE_CODE, new PointSerializer());
        serializerByGeoshapeTypeCode.put(GeoshapeGraphBinaryConstants.GEOSHAPE_CIRCLE_TYPE_CODE, new CircleSerializer());
        serializerByGeoshapeTypeCode.put(GeoshapeGraphBinaryConstants.GEOSHAPE_BOX_TYPE_CODE, new BoxSerializer());
        serializerByGeoshapeTypeCode.put(GeoshapeGraphBinaryConstants.GEOSHAPE_LINE_TYPE_CODE, new LineSerializer());
        serializerByGeoshapeTypeCode.put(GeoshapeGraphBinaryConstants.GEOSHAPE_POLYGON_TYPE_CODE, new PolygonSerializer());
        serializerByGeoshapeTypeCode.put(GeoshapeGraphBinaryConstants.GEOSHAPE_MULTI_POINT_TYPE_CODE, new MultiPointSerializer());
        serializerByGeoshapeTypeCode.put(GeoshapeGraphBinaryConstants.GEOSHAPE_MULTI_LINE_TYPE_CODE, new MultiLineSerializer());
        serializerByGeoshapeTypeCode.put(GeoshapeGraphBinaryConstants.GEOSHAPE_MULTI_POLYGON_TYPE_CODE, new MultiPolygonSerializer());
        serializerByGeoshapeTypeCode.put(GeoshapeGraphBinaryConstants.GEOSHAPE_GEOMETRY_COLLECTION_TYPE_CODE, new GeometryCollectionSerializer());


        serializerByGeoshapeType.put(Geoshape.Type.POINT, new PointSerializer());
        serializerByGeoshapeType.put(Geoshape.Type.CIRCLE, new CircleSerializer());
        serializerByGeoshapeType.put(Geoshape.Type.BOX, new BoxSerializer());
        serializerByGeoshapeType.put(Geoshape.Type.LINE, new LineSerializer());
        serializerByGeoshapeType.put(Geoshape.Type.POLYGON, new PolygonSerializer());
        serializerByGeoshapeType.put(Geoshape.Type.MULTIPOINT, new MultiPointSerializer());
        serializerByGeoshapeType.put(Geoshape.Type.MULTILINESTRING, new MultiLineSerializer());
        serializerByGeoshapeType.put(Geoshape.Type.MULTIPOLYGON, new MultiPolygonSerializer());
        serializerByGeoshapeType.put(Geoshape.Type.GEOMETRYCOLLECTION, new GeometryCollectionSerializer());
    }

    @Override
    public Geoshape readNonNullableValue(final Buffer buffer, final GraphBinaryReader context) throws IOException {
        final byte formatVersion = buffer.readByte();
        if (formatVersion != GeoshapeGraphBinaryConstants.GEOSHAPE_FORMAT_VERSION) {
            throw new SerializationException("Geoshape format " + formatVersion + " not supported");
        }

        final int geoshapeTypeCode = buffer.readInt();
        final GeoshapeTypeSerializer serializer = serializerByGeoshapeTypeCode.get(geoshapeTypeCode);
        if (serializer == null) {
            throw new SerializationException("Geoshape type code " + geoshapeTypeCode + " not supported");
        }
        return serializer.readNonNullableGeoshapeValue(buffer, context);

    }

    @Override
    public void writeNonNullableValue(final Geoshape geoshape, final Buffer buffer, final GraphBinaryWriter context) throws IOException {
        final Geoshape.Type type = geoshape.getType();

        final GeoshapeTypeSerializer serializer = serializerByGeoshapeType.get(type);
        if (serializer == null) {
            throw new SerializationException("Geoshape type " + type + " not supported");
        }
        buffer.writeByte(GeoshapeGraphBinaryConstants.GEOSHAPE_FORMAT_VERSION);

        serializer.writeNonNullableValue(geoshape, buffer, context);
    }
}
