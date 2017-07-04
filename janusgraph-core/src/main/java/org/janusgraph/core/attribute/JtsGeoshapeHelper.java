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

import org.locationtech.spatial4j.context.jts.DatelineRule;
import org.locationtech.spatial4j.context.jts.JtsSpatialContext;
import org.locationtech.spatial4j.context.jts.JtsSpatialContextFactory;
import org.locationtech.spatial4j.io.GeoJSONReader;
import org.locationtech.spatial4j.io.WKTReader;
import org.locationtech.spatial4j.io.jts.JtsBinaryCodec;
import org.locationtech.spatial4j.io.jts.JtsGeoJSONWriter;
import org.locationtech.spatial4j.io.jts.JtsWKTWriter;
import org.locationtech.spatial4j.shape.Shape;
import org.locationtech.spatial4j.shape.ShapeFactory.PolygonBuilder;
import org.locationtech.spatial4j.shape.jts.JtsGeometry;

import com.google.common.base.Preconditions;
import com.vividsolutions.jts.geom.Coordinate;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

/**
 * Extension of default spatial context and associated I/O operations to use the Java Topology Suite (JTS), which adds
 * support for polygon, multi-polygon and geometry collection with polygons.
 */
public class JtsGeoshapeHelper extends GeoshapeHelper {

    public JtsGeoshapeHelper() {
        JtsSpatialContextFactory jtsFactory = new JtsSpatialContextFactory();
        jtsFactory.geo = true;
        jtsFactory.useJtsPoint = false;
        jtsFactory.useJtsLineString = true;
        // TODO: Use default dateline rule and update to support multiline/polygon to resolve wrapping issues
        jtsFactory.datelineRule = DatelineRule.none;
        JtsSpatialContext context = new JtsSpatialContext(jtsFactory);

        super.context = context;
        super.factory = jtsFactory;
        wktReader = new WKTReader(context, factory);
        wktWriter = new JtsWKTWriter(context, jtsFactory);
        geojsonReader = new GeoJSONReader(context, factory);
        geojsonWriter = new JtsGeoJSONWriter(context, jtsFactory);
        binaryCodec = new JtsBinaryCodec(context, jtsFactory);
    }

    public Geoshape geoshape(com.vividsolutions.jts.geom.Geometry geometry) {
        return new Geoshape(((JtsSpatialContext) context).getShapeFactory().makeShapeFromGeometry(geometry));
    }

    @Override
    public Shape readGeometry(DataInputStream dataInput) throws IOException {
        return ((JtsBinaryCodec) binaryCodec).readJtsGeom(dataInput);
    }

    @Override
    public void write(DataOutputStream dataOutput, Geoshape attribute) throws IOException {
        if (attribute.getShape() instanceof JtsGeometry) {
            ((JtsBinaryCodec) binaryCodec).writeJtsGeom(dataOutput, attribute.getShape());
        } else {
            binaryCodec.writeShape(dataOutput, attribute.getShape());
        }
    }

    @Override
    public Geoshape polygon(List<double[]> coordinates) {
        Preconditions.checkArgument(coordinates.size() >= 4, "Too few coordinate pairs provided");
        Preconditions.checkArgument(coordinates.get(0) != coordinates.get(coordinates.size()-1), "Polygon is not closed");
        final PolygonBuilder builder = this.getContext().getShapeFactory().polygon();
        for (double[] coordinate : coordinates) {
            Preconditions.checkArgument(coordinate.length==2 && Geoshape.isValidCoordinate(coordinate[1], coordinate[0]), "Invalid coordinate provided");
            builder.pointXY(coordinate[0], coordinate[1]);
        }
        return new Geoshape(builder.build());
    }

    @Override
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
        } else {
            type = super.getType(shape);
        }
        return type;
    }

    @Override
    public int size(Shape shape) {
        switch(getType(shape)) {
            case LINE:
            case POLYGON:
            case MULTIPOINT:
            case MULTILINESTRING:
            case MULTIPOLYGON:
                return ((JtsGeometry) shape).getGeom().getCoordinates().length;
            default:
                return super.size(shape);
        }
    }

    @Override
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
            default:
                return super.getPoint(geoshape, position);
        }
    }

    @Override
    public boolean isJts(Shape shape) {
        return shape instanceof JtsGeometry;
    }

}
