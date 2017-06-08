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

package org.janusgraph.core.attribute;

import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.context.SpatialContextFactory;
import org.locationtech.spatial4j.io.BinaryCodec;
import org.locationtech.spatial4j.io.GeoJSONReader;
import org.locationtech.spatial4j.io.GeoJSONWriter;
import org.locationtech.spatial4j.io.WKTReader;
import org.locationtech.spatial4j.io.WKTWriter;
import org.locationtech.spatial4j.shape.Circle;
import org.locationtech.spatial4j.shape.Point;
import org.locationtech.spatial4j.shape.Rectangle;
import org.locationtech.spatial4j.shape.Shape;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

/**
 * Base class for default implementation of spatial context and associated I/O operations supporting point, circle and
 * rectangle shapes.
 */
public class GeoshapeHelper {

    protected SpatialContext context;

    protected WKTReader wktReader;

    protected WKTWriter wktWriter;

    protected GeoJSONReader geojsonReader;

    protected GeoJSONWriter geojsonWriter;

    protected BinaryCodec binaryCodec;

    public GeoshapeHelper() {
        SpatialContextFactory factory = new SpatialContextFactory();
        factory.geo = true;
        context = new SpatialContext(factory);
        wktReader = new WKTReader(context, factory);
        wktWriter = new WKTWriter();
        geojsonReader = new GeoJSONReader(context, factory);
        geojsonWriter = new GeoJSONWriter(context, factory);
        binaryCodec = new BinaryCodec(context, factory);
    }

    public Shape readGeometry(DataInputStream dataInput) throws IOException {
        throw new UnsupportedOperationException("JTS is required for this operation");
    }

    public void write(DataOutputStream dataOutput, Geoshape attribute) throws IOException {
        binaryCodec.writeShape(dataOutput, attribute.getShape());
    }

    public Geoshape polygon(List<double[]> coordinates) {
        throw new UnsupportedOperationException("JTS is required for this operation");
    }

    public Geoshape.Type getType(Shape shape) {
        final Geoshape.Type type;
        if (Point.class.isAssignableFrom(shape.getClass())) {
            type = Geoshape.Type.POINT;
        } else if (Circle.class.isAssignableFrom(shape.getClass())) {
            type = Geoshape.Type.CIRCLE;
        } else if (Rectangle.class.isAssignableFrom(shape.getClass())) {
            type = Geoshape.Type.BOX;
        } else {
            throw new IllegalStateException("Unrecognized shape type");
        }
        return type;
    }

    public int size(Shape shape) {
        switch(getType(shape)) {
            case POINT: return 1;
            case CIRCLE: return 1;
            case BOX: return 2;
            default: throw new IllegalStateException("size() not supported for type: " + getType(shape));
        }
    }

    public Geoshape.Point getPoint(Geoshape geoshape, int position) {
        Shape shape = geoshape.getShape();
        if (position<0 || position>=size(shape)) throw new ArrayIndexOutOfBoundsException("Invalid position: " + position);
        switch(getType(shape)) {
            case POINT:
            case CIRCLE:
                return geoshape.getPoint();
            case BOX:
                if (position == 0)
                    return new Geoshape.Point(shape.getBoundingBox().getMinY(), shape.getBoundingBox().getMinX());
                else
                    return new Geoshape.Point(shape.getBoundingBox().getMaxY(), shape.getBoundingBox().getMaxX());
            default:
                throw new IllegalStateException("getPoint(int) not supported for type: " + getType(shape));
        }
    }

    public boolean isJts(Shape shape) {
        return false;
    }

    public SpatialContext getContext() {
        return context;
    }

    public WKTReader getWktReader() {
        return wktReader;
    }

    public WKTWriter getWktWriter() {
        return wktWriter;
    }

    public GeoJSONReader getGeojsonReader() {
        return geojsonReader;
    }

    public GeoJSONWriter getGeojsonWriter() {
        return geojsonWriter;
    }

    public BinaryCodec getBinaryCodec() {
        return binaryCodec;
    }

}
