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

import com.google.common.base.Preconditions;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.context.jts.DatelineRule;
import org.locationtech.spatial4j.context.jts.JtsSpatialContext;
import org.locationtech.spatial4j.context.jts.JtsSpatialContextFactory;
import org.locationtech.spatial4j.io.GeoJSONReader;
import org.locationtech.spatial4j.io.GeoJSONWriter;
import org.locationtech.spatial4j.io.WKTReader;
import org.locationtech.spatial4j.io.WKTWriter;
import org.locationtech.spatial4j.io.jts.JtsBinaryCodec;
import org.locationtech.spatial4j.io.jts.JtsGeoJSONWriter;
import org.locationtech.spatial4j.io.jts.JtsWKTWriter;
import org.locationtech.spatial4j.shape.Circle;
import org.locationtech.spatial4j.shape.Point;
import org.locationtech.spatial4j.shape.Rectangle;
import org.locationtech.spatial4j.shape.Shape;
import org.locationtech.spatial4j.shape.ShapeCollection;
import org.locationtech.spatial4j.shape.ShapeFactory.PolygonBuilder;
import org.locationtech.spatial4j.shape.impl.BufferedLineString;
import org.locationtech.spatial4j.shape.jts.JtsGeometry;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Class for spatial context and associated I/O operations including the Java Topology Suite (JTS) to support polygon,
 * multi-polygon and geometry collections with polygons.
 */
public class JtsGeoshapeHelper {

    protected JtsSpatialContext context;

    protected WKTReader wktReader;

    protected WKTWriter wktWriter;

    protected GeoJSONReader geojsonReader;

    protected GeoJSONWriter geojsonWriter;

    protected JtsBinaryCodec binaryCodec;

    protected JtsSpatialContextFactory factory;

    public JtsGeoshapeHelper() {
        factory = new JtsSpatialContextFactory();
        factory.geo = true;
        factory.useJtsPoint = false;
        factory.useJtsLineString = true;
        // TODO: Use default dateline rule and update to support multiline/polygon to resolve wrapping issues
        factory.datelineRule = DatelineRule.none;
        context = new JtsSpatialContext(factory);

        wktReader = new WKTReader(context, factory);
        wktWriter = new JtsWKTWriter(context, factory);
        geojsonReader = new GeoJSONReader(context, factory);
        geojsonWriter = new JtsGeoJSONWriter(context, factory);
        binaryCodec = new JtsBinaryCodec(context, factory);
    }

    public Geoshape geoshape(org.locationtech.jts.geom.Geometry geometry) {
        return new Geoshape(context.getShapeFactory().makeShapeFromGeometry(geometry));
    }

    public Shape readShape(DataInputStream dataInput) throws IOException {
        if (dataInput.readByte() == 0) {
            // note geometries written with writeJtsGeom cannot be deserialized with readShape so to maintain backwards
            // compatibility with previously written geometries must maintain read/writeJtsGeom for the JtsGeometry case
            return binaryCodec.readJtsGeom(dataInput);
        } else {
            return binaryCodec.readShape(dataInput);
        }
    }

    public void write(DataOutputStream dataOutput, Geoshape attribute) throws IOException {
        if (isJts(attribute.getShape())) {
            dataOutput.writeByte(0);
            // note geometries written with writeJtsGeom cannot be deserialized with readShape so to maintain backwards
            // compatibility with previously written geometries must maintain read/writeJtsGeom for the JtsGeometry case
            binaryCodec.writeJtsGeom(dataOutput, attribute.getShape());
        } else {
            dataOutput.writeByte(1);
            binaryCodec.writeShape(dataOutput, attribute.getShape());
        }
    }

    public Geoshape polygon(List<double[]> coordinates) {
        Preconditions.checkArgument(coordinates.size() >= 4, "Too few coordinate pairs provided");
        Preconditions.checkArgument(Arrays.equals(coordinates.get(0), coordinates.get(coordinates.size()-1)), "Polygon is not closed");
        final PolygonBuilder builder = this.getContext().getShapeFactory().polygon();
        for (double[] coordinate : coordinates) {
            Preconditions.checkArgument(coordinate.length==2 && Geoshape.isValidCoordinate(coordinate[1], coordinate[0]), "Invalid coordinate provided");
            builder.pointXY(coordinate[0], coordinate[1]);
        }
        return new Geoshape(builder.build());
    }

    @SuppressWarnings("unchecked")
    public Geoshape.Type getType(Shape shape) {
        final Geoshape.Type type;
        if (JtsGeometry.class.isAssignableFrom(shape.getClass()) && "LineString".equals(((JtsGeometry) shape).getGeom().getGeometryType())) {
            type = Geoshape.Type.LINE;
        } else if (JtsGeometry.class.isAssignableFrom(shape.getClass())) {
            try {
                type = Geoshape.Type.fromGson((((JtsGeometry) shape).getGeom().getGeometryType()));
            } catch (IllegalArgumentException e) {
                throw new IllegalStateException("Unrecognized shape type");
            }
        } else if (Point.class.isAssignableFrom(shape.getClass())) {
            type = Geoshape.Type.POINT;
        } else if (Circle.class.isAssignableFrom(shape.getClass())) {
            type = Geoshape.Type.CIRCLE;
        } else if (Rectangle.class.isAssignableFrom(shape.getClass())) {
            type = Geoshape.Type.BOX;
        } else if (BufferedLineString.class.isAssignableFrom(shape.getClass())) {
            type = Geoshape.Type.LINE;
        } else if (ShapeCollection.class.isAssignableFrom(shape.getClass())) {
            final Set<Geoshape.Type> types = ((ShapeCollection<? extends Shape>) shape).getShapes().stream()
                .map(this::getType)
                .collect(Collectors.toSet());
            switch (types.size() == 1 ? types.iterator().next() : Geoshape.Type.GEOMETRYCOLLECTION) {
                case POINT:
                    type = Geoshape.Type.MULTIPOINT;
                    break;
                case LINE:
                    type = Geoshape.Type.MULTILINESTRING;
                    break;
                case POLYGON:
                    type = Geoshape.Type.MULTIPOLYGON;
                    break;
                default:
                    type = Geoshape.Type.GEOMETRYCOLLECTION;
                    break;
            }
        } else {
            throw new IllegalStateException("Unrecognized shape type: " + shape.getClass());
        }

        return type;
    }

    public int size(Shape shape) {
        switch(getType(shape)) {
            case LINE:
            case POLYGON:
            case MULTIPOINT:
            case MULTILINESTRING:
            case MULTIPOLYGON:
                return ((JtsGeometry) shape).getGeom().getCoordinates().length;
            case POINT:
                return 1;
            case CIRCLE:
                return 1;
            case BOX:
                return 2;
            case GEOMETRYCOLLECTION:
                return ((ShapeCollection<?>) shape).getShapes().stream().map(s -> (Shape) s).mapToInt(this::size).sum();
            default:
                throw new IllegalStateException("size() not supported for type: " + getType(shape));
        }
    }

    public Geoshape.Point getPoint(Geoshape geoshape, int position) {
        Shape shape = geoshape.getShape();
        if (position < 0 || position >= size(shape)) throw new ArrayIndexOutOfBoundsException("Invalid position: " + position);
        switch(getType(shape)) {
            case LINE:
            case POLYGON:
            case MULTIPOINT:
            case MULTILINESTRING:
            case MULTIPOLYGON:
                Coordinate coordinate = ((JtsGeometry) shape).getGeom().getCoordinates()[position];
                return new Geoshape.Point(coordinate.y, coordinate.x);
            case POINT:
            case CIRCLE:
                return geoshape.getPoint();
            case BOX:
                if (position == 0)
                    return new Geoshape.Point(shape.getBoundingBox().getMinY(), shape.getBoundingBox().getMinX());
                else
                    return new Geoshape.Point(shape.getBoundingBox().getMaxY(), shape.getBoundingBox().getMaxX());
            case GEOMETRYCOLLECTION:
                return ((ShapeCollection<Shape>) shape).getShapes().stream()
                    .flatMap(internShape -> IntStream.range(0, size(internShape))
                        .mapToObj(i -> new AbstractMap.SimpleImmutableEntry<>(internShape, i)))
                    .skip(position)
                    .findFirst()
                    .map(entry -> getPoint(new Geoshape(entry.getKey()), entry.getValue()))
                    .orElse(null);
            default:
                throw new IllegalStateException("getPoint(int) not supported for type: " + getType(shape));
        }
    }

    public boolean isJts(Shape shape) {
        return shape instanceof JtsGeometry;
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

    public JtsBinaryCodec getBinaryCodec() {
        return binaryCodec;
    }

}
