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
import org.locationtech.spatial4j.context.jts.DatelineRule;
import org.locationtech.spatial4j.context.jts.JtsSpatialContext;
import org.locationtech.spatial4j.context.jts.JtsSpatialContextFactory;
import org.locationtech.spatial4j.io.GeoJSONReader;
import org.locationtech.spatial4j.io.WKTReader;
import org.locationtech.spatial4j.io.jts.JtsBinaryCodec;
import org.locationtech.spatial4j.io.jts.JtsGeoJSONWriter;
import org.locationtech.spatial4j.io.jts.JtsWKTWriter;
import org.locationtech.spatial4j.shape.Shape;
import org.locationtech.spatial4j.shape.jts.JtsGeometry;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

/**
 * Extension of default spatial context and associated I/O operations to use the Java Topology Suite (JTS), which adds
 * support for line and polygon shapes.
 */
public class JtsGeoshapeHelper extends GeoshapeHelper {

    public JtsGeoshapeHelper() {
        JtsSpatialContextFactory factory = new JtsSpatialContextFactory();
        factory.geo = true;
        factory.useJtsPoint = false;
        factory.useJtsLineString = true;
        // TODO: Use default dateline rule and update to support multiline/polygon to resolve wrapping issues
        factory.datelineRule = DatelineRule.none;
        JtsSpatialContext context = new JtsSpatialContext(factory);

        super.context = context;
        wktReader = new WKTReader(context, factory);
        wktWriter = new JtsWKTWriter(context, factory);
        geojsonReader = new GeoJSONReader(context, factory);
        geojsonWriter = new JtsGeoJSONWriter(context, factory);
        binaryCodec = new JtsBinaryCodec(context, factory);
    }

    public Geoshape geoshape(com.vividsolutions.jts.geom.Geometry geometry) {
        return new Geoshape(((JtsSpatialContext) context).makeShape(geometry));
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
        Coordinate[] points = new Coordinate[coordinates.size()];
        for (int i=0; i<coordinates.size(); i++) {
            double[] coordinate = coordinates.get(i);
            Preconditions.checkArgument(coordinate.length==2 && Geoshape.isValidCoordinate(coordinate[1],coordinate[0]),"Invalid coordinate provided");
            points[i] = new Coordinate(coordinate[0],  coordinate[1]);
        }
        GeometryFactory factory = new GeometryFactory();
        return new Geoshape(((JtsSpatialContext) context).makeShape(factory.createPolygon(points)));
    }

    @Override
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
                return ((JtsGeometry) shape).getGeom().getCoordinates().length;
            default:
                return super.size(shape);
        }
    }

    @Override
    public Geoshape.Point getPoint(Geoshape geoshape, int position) {
        Shape shape = geoshape.getShape();
        if (position<0 || position>=size(shape)) throw new ArrayIndexOutOfBoundsException("Invalid position: " + position);
        switch(getType(shape)) {
            case LINE:
            case POLYGON:
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
