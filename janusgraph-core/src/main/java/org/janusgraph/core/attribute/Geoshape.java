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
import com.google.common.primitives.Doubles;
import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.distance.DistanceUtils;
import com.spatial4j.core.shape.Shape;
import com.spatial4j.core.shape.SpatialRelation;
import org.janusgraph.diskstorage.ScanBuffer;
import org.janusgraph.diskstorage.WriteBuffer;
import org.janusgraph.graphdb.database.idhandling.VariableLong;
import org.apache.commons.lang.builder.HashCodeBuilder;

import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONTokens;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONUtil;
import org.apache.tinkerpop.shaded.jackson.core.JsonGenerator;
import org.apache.tinkerpop.shaded.jackson.core.JsonParser;
import org.apache.tinkerpop.shaded.jackson.core.JsonProcessingException;
import org.apache.tinkerpop.shaded.jackson.databind.DeserializationContext;
import org.apache.tinkerpop.shaded.jackson.databind.SerializerProvider;
import org.apache.tinkerpop.shaded.jackson.databind.deser.std.StdDeserializer;
import org.apache.tinkerpop.shaded.jackson.databind.jsontype.TypeSerializer;
import org.apache.tinkerpop.shaded.jackson.databind.ser.std.StdSerializer;
import org.apache.tinkerpop.shaded.kryo.Kryo;
import org.apache.tinkerpop.shaded.kryo.Serializer;
import org.apache.tinkerpop.shaded.kryo.io.Input;
import org.apache.tinkerpop.shaded.kryo.io.Output;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.*;
import java.util.stream.Collectors;

/**
 * A generic representation of a geographic shape, which can either be a single point,
 * circle, box, or polygon. Use {@link #getType()} to determine the type of shape of a particular Geoshape object.
 * Use the static constructor methods to create the desired geoshape.
 *
 * Note, polygons are not yet supported.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class Geoshape {

    private static String FIELD_TYPE = "type";
    private static String FIELD_COORDINATES = "coordinates";
    private static String FIELD_RADIUS = "radius";

    private static final SpatialContext CTX = SpatialContext.GEO;

    /**
     * The Type of a shape: a point, box, circle, or polygon.
     */
    public enum Type {
        POINT("Point"),
        BOX("Box"),
        CIRCLE("Circle"),
        POLYGON("Polygon");

        private final String gsonName;

        Type(String gsonName) {
            this.gsonName = gsonName;
        }

        public boolean gsonEquals(String otherGson) {
            return otherGson != null && gsonName.equals(otherGson);
        }

        public static Type fromGson(String gsonShape) {
            return Type.valueOf(gsonShape.toUpperCase());
        }

        @Override
        public String toString() {
            return gsonName;
        }
    }

    //coordinates[0] = latitudes, coordinates[1] = longitudes
    private final float[][] coordinates;

    private Geoshape() {
        coordinates = null;
    }

    private Geoshape(final float[][] coordinates) {
        Preconditions.checkArgument(coordinates!=null && coordinates.length==2);
        Preconditions.checkArgument(coordinates[0].length==coordinates[1].length && coordinates[0].length>0);
        for (int i=0;i<coordinates[0].length;i++) {
            if (Float.isNaN(coordinates[0][i])) Preconditions.checkArgument(i==1 && coordinates.length==2 && coordinates[1][i]>0);
            else Preconditions.checkArgument(isValidCoordinate(coordinates[0][i],coordinates[1][i]));
        }
        this.coordinates=coordinates;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(coordinates[0]).append(coordinates[1]).toHashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (this==other) return true;
        else if (other==null) return false;
        else if (!getClass().isInstance(other)) return false;
        Geoshape oth = (Geoshape)other;
        Preconditions.checkArgument(coordinates.length==2 && oth.coordinates.length==2);
        for (int i=0;i<coordinates.length;i++) {
            if (coordinates[i].length!=oth.coordinates[i].length) return false;
            for (int j=0;j<coordinates[i].length;j++) {
                if (Float.isNaN(coordinates[i][j]) && Float.isNaN(oth.coordinates[i][j])) continue;
                if (coordinates[i][j]!=oth.coordinates[i][j]) return false;
            }
        }
        return true;
    }

    @Override
    public String toString() {
        Type type = getType();
        StringBuilder s = new StringBuilder();
        s.append(type.toString().toLowerCase());
        switch (type) {
            case POINT:
                s.append(getPoint().toString());
                break;
            case CIRCLE:
                s.append(getPoint().toString()).append(":").append(getRadius());
                break;
            default:
                s.append("[");
                for (int i=0;i<size();i++) {
                    if (i>0) s.append(",");
                    s.append(getPoint(i));
                }
                s.append("]");
        }
        return s.toString();
    }

    /**
     * Returns the {@link Type} of this geoshape.
     *
     * @return
     */
    public Type getType() {
        if (coordinates[0].length==1) return Type.POINT;
        else if (coordinates[0].length>2) return Type.POLYGON;
        else { //coordinates[0].length==2
            if (Float.isNaN(coordinates[0][1])) return Type.CIRCLE;
            else return Type.BOX;
        }
    }

    /**
     * Returns the number of points comprising this geoshape. A point and circle have only one point (center of cricle),
     * a box has two points (the south-west and north-east corners) and a polygon has a variable number of points (>=3).
     *
     * @return
     */
    public int size() {
        switch(getType()) {
            case POINT: return 1;
            case CIRCLE: return 1;
            case BOX: return 2;
            case POLYGON: return coordinates[0].length;
            default: throw new IllegalStateException("Unrecognized type: " + getType());
        }
    }

    /**
     * Returns the point at the given position. The position must be smaller than {@link #size()}.
     *
     * @param position
     * @return
     */
    public Point getPoint(int position) {
        if (position<0 || position>=size()) throw new ArrayIndexOutOfBoundsException("Invalid position: " + position);
        return new Point(coordinates[0][position],coordinates[1][position]);
    }

    /**
     * Returns the singleton point of this shape. Only applicable for point and circle shapes.
     *
     * @return
     */
    public Point getPoint() {
        Preconditions.checkArgument(size()==1,"Shape does not have a single point");
        return getPoint(0);
    }

    /**
     * Returns the radius in kilometers of this circle. Only applicable to circle shapes.
     * @return
     */
    public float getRadius() {
        Preconditions.checkArgument(getType()==Type.CIRCLE,"This shape is not a circle");
        return coordinates[1][1];
    }

    private SpatialRelation getSpatialRelation(Geoshape other) {
        Preconditions.checkNotNull(other);
        return convert2Spatial4j().relate(other.convert2Spatial4j());
    }

    public boolean intersect(Geoshape other) {
        SpatialRelation r = getSpatialRelation(other);
        return r==SpatialRelation.INTERSECTS || r==SpatialRelation.CONTAINS || r==SpatialRelation.WITHIN;
    }

    public boolean within(Geoshape outer) {
        return getSpatialRelation(outer)==SpatialRelation.WITHIN;
    }

    public boolean disjoint(Geoshape other) {
        return getSpatialRelation(other)==SpatialRelation.DISJOINT;
    }

    /**
     * Converts this shape into its equivalent Spatial4j {@link Shape}.
     * @return
     */
    public Shape convert2Spatial4j() {
        switch(getType()) {
            case POINT: return getPoint().getSpatial4jPoint();
            case CIRCLE: return CTX.makeCircle(getPoint(0).getSpatial4jPoint(), DistanceUtils.dist2Degrees(getRadius(), DistanceUtils.EARTH_MEAN_RADIUS_KM));
            case BOX: return CTX.makeRectangle(getPoint(0).getSpatial4jPoint(),getPoint(1).getSpatial4jPoint());
            case POLYGON: throw new UnsupportedOperationException("Not yet supported");
            default: throw new IllegalStateException("Unrecognized type: " + getType());
        }
    }


    /**
     * Constructs a point from its latitude and longitude information
     * @param latitude
     * @param longitude
     * @return
     */
    public static final Geoshape point(final float latitude, final float longitude) {
        Preconditions.checkArgument(isValidCoordinate(latitude,longitude),"Invalid coordinate provided");
        return new Geoshape(new float[][]{ new float[]{latitude}, new float[]{longitude}});
    }

    /**
     * Constructs a point from its latitude and longitude information
     * @param latitude
     * @param longitude
     * @return
     */
    public static final Geoshape point(final double latitude, final double longitude) {
        return point((float)latitude,(float)longitude);
    }

    /**
     * Constructs a circle from a given center point and a radius in kilometer
     * @param latitude
     * @param longitude
     * @param radiusInKM
     * @return
     */
    public static final Geoshape circle(final float latitude, final float longitude, final float radiusInKM) {
        Preconditions.checkArgument(isValidCoordinate(latitude,longitude),"Invalid coordinate provided");
        Preconditions.checkArgument(radiusInKM>0,"Invalid radius provided [%s]",radiusInKM);
        return new Geoshape(new float[][]{ new float[]{latitude, Float.NaN}, new float[]{longitude, radiusInKM}});
    }

    /**
     * Constructs a circle from a given center point and a radius in kilometer
     * @param latitude
     * @param longitude
     * @param radiusInKM
     * @return
     */
    public static final Geoshape circle(final double latitude, final double longitude, final double radiusInKM) {
        return circle((float)latitude,(float)longitude,(float)radiusInKM);
    }

    /**
     * Constructs a new box shape which is identified by its south-west and north-east corner points
     * @param southWestLatitude
     * @param southWestLongitude
     * @param northEastLatitude
     * @param northEastLongitude
     * @return
     */
    public static final Geoshape box(final float southWestLatitude, final float southWestLongitude,
                                     final float northEastLatitude, final float northEastLongitude) {
        Preconditions.checkArgument(isValidCoordinate(southWestLatitude,southWestLongitude),"Invalid south-west coordinate provided");
        Preconditions.checkArgument(isValidCoordinate(northEastLatitude,northEastLongitude),"Invalid north-east coordinate provided");
        return new Geoshape(new float[][]{ new float[]{southWestLatitude, northEastLatitude}, new float[]{southWestLongitude, northEastLongitude}});
    }

    /**
     * Constructs a new box shape which is identified by its south-west and north-east corner points
     * @param southWestLatitude
     * @param southWestLongitude
     * @param northEastLatitude
     * @param northEastLongitude
     * @return
     */
    public static final Geoshape box(final double southWestLatitude, final double southWestLongitude,
                                     final double northEastLatitude, final double northEastLongitude) {
        return box((float)southWestLatitude,(float)southWestLongitude,(float)northEastLatitude,(float)northEastLongitude);
    }

    /**
     * Whether the given coordinates mark a point on earth.
     * @param latitude
     * @param longitude
     * @return
     */
    public static final boolean isValidCoordinate(final float latitude, final float longitude) {
        return latitude>=-90.0 && latitude<=90.0 && longitude>=-180.0 && longitude<=180.0;
    }

    /**
     * A single point representation. A point is identified by its coordinate on the earth sphere using the spherical
     * system of latitudes and longitudes.
     */
    public static final class Point {

        private final float longitude;
        private final float latitude;

        /**
         * Constructs a point with the given latitude and longitude
         * @param latitude Between -90 and 90 degrees
         * @param longitude Between -180 and 180 degrees
         */
        Point(float latitude, float longitude) {
            this.longitude = longitude;
            this.latitude = latitude;
        }

        /**
         * Longitude of this point
         * @return
         */
        public float getLongitude() {
            return longitude;
        }

        /**
         * Latitude of this point
         * @return
         */
        public float getLatitude() {
            return latitude;
        }

        private com.spatial4j.core.shape.Point getSpatial4jPoint() {
            return CTX.makePoint(longitude,latitude);
        }

        /**
         * Returns the distance to another point in kilometers
         *
         * @param other Point
         * @return
         */
        public double distance(Point other) {
            return DistanceUtils.degrees2Dist(CTX.getDistCalc().distance(getSpatial4jPoint(),other.getSpatial4jPoint()),DistanceUtils.EARTH_MEAN_RADIUS_KM);
        }

        @Override
        public String toString() {
            return "["+latitude+","+longitude+"]";
        }

        @Override
        public int hashCode() {
            return new HashCodeBuilder().append(latitude).append(longitude).toHashCode();
        }

        @Override
        public boolean equals(Object other) {
            if (this==other) return true;
            else if (other==null) return false;
            else if (!getClass().isInstance(other)) return false;
            Point oth = (Point)other;
            return latitude==oth.latitude && longitude==oth.longitude;
        }

    }

    /**
     * @author Matthias Broecheler (me@matthiasb.com)
     */
    public static class GeoshapeSerializer implements AttributeSerializer<Geoshape> {

        @Override
        public void verifyAttribute(Geoshape value) {
            //All values of Geoshape are valid
        }

        @Override
        public Geoshape convert(Object value) {

            if(value instanceof Map) {
                return convertGeoJson(value);
            }

            if(value instanceof Collection) {
                value = convertCollection((Collection<Object>) value);
            }

            if (value.getClass().isArray() && (value.getClass().getComponentType().isPrimitive() ||
                    Number.class.isAssignableFrom(value.getClass().getComponentType())) ) {
                Geoshape shape = null;
                int len= Array.getLength(value);
                double[] arr = new double[len];
                for (int i=0;i<len;i++) arr[i]=((Number)Array.get(value,i)).doubleValue();
                if (len==2) shape= point(arr[0],arr[1]);
                else if (len==3) shape= circle(arr[0],arr[1],arr[2]);
                else if (len==4) shape= box(arr[0],arr[1],arr[2],arr[3]);
                else throw new IllegalArgumentException("Expected 2-4 coordinates to create Geoshape, but given: " + value);
                return shape;
            } else if (value instanceof String) {
                String[] components=null;
                for (String delimiter : new String[]{",",";"}) {
                    components = ((String)value).split(delimiter);
                    if (components.length>=2 && components.length<=4) break;
                    else components=null;
                }
                Preconditions.checkArgument(components!=null,"Could not parse coordinates from string: %s",value);
                double[] coords = new double[components.length];
                try {
                    for (int i=0;i<components.length;i++) {
                        coords[i]=Double.parseDouble(components[i]);
                    }
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("Could not parse coordinates from string: " + value, e);
                }
                return convert(coords);
            } else return null;
        }


        private double[] convertCollection(Collection<Object> c) {

            List<Double> numbers = c.stream().map(o -> {
                if (!(o instanceof Number)) {
                    throw new IllegalArgumentException("Collections may only contain numbers to create a Geoshape");
                }
                return ((Number) o).doubleValue();
            }).collect(Collectors.toList());
            return Doubles.toArray(numbers);
        }

        private Geoshape convertGeoJson(Object value) {
            //Note that geoJson is long,lat
            try {
                Map<String, Object> map = (Map) value;
                String type = (String) map.get("type");
                if("Point".equals(type) || "Circle".equals(type) || "Polygon".equals(type)) {
                    return convertGeometry(map);
                }
                else if("Feature".equals(type)) {
                    Map<String, Object> geometry = (Map) map.get("geometry");
                    return convertGeometry(geometry);
                }
                throw new IllegalArgumentException("Only Point, Circle, Polygon or feature types are supported");
            } catch (ClassCastException e) {
                throw new IllegalArgumentException("GeoJSON was unparsable");
            }

        }

        private Geoshape convertGeometry(Map<String, Object> geometry) {
            String type = (String) geometry.get("type");
            List<Object> coordinates = (List) geometry.get("coordinates");
            //Either this is a single point or a collection of points

            if ("Point".equals(type)) {
                double[] parsedCoordinates = convertCollection(coordinates);
                return point(parsedCoordinates[1], parsedCoordinates[0]);
            } else if ("Circle".equals(type)) {
                Number radius = (Number) geometry.get("radius");
                if (radius == null) {
                    throw new IllegalArgumentException("GeoJSON circles require a radius");
                }
                double[] parsedCoordinates = convertCollection(coordinates);
                return circle(parsedCoordinates[1], parsedCoordinates[0], radius.doubleValue());
            } else if ("Polygon".equals(type)) {
                if (coordinates.size() != 4) {
                    throw new IllegalArgumentException("GeoJSON polygons are only supported if they form a box");
                }
                List<double[]> polygon = (List<double[]>) coordinates.stream().map(o -> convertCollection((Collection) o)).collect(Collectors.toList());

                double[] p0 = polygon.get(0);
                double[] p1 = polygon.get(1);
                double[] p2 = polygon.get(2);
                double[] p3 = polygon.get(3);

                //This may be a clockwise or counterclockwise polygon, we have to verify that it is a box
                if ((p0[0] == p1[0] && p1[1] == p2[1] && p2[0] == p3[0] && p3[1] == p0[1]) ||
                        (p0[1] == p1[1] && p1[0] == p2[0] && p2[1] == p3[1] && p3[0] == p0[0])) {
                    return box(min(p0[1], p1[1], p2[1], p3[1]), min(p0[0], p1[0], p2[0], p3[0]), max(p0[1], p1[1], p2[1], p3[1]), max(p0[0], p1[0], p2[0], p3[0]));
                }

                throw new IllegalArgumentException("GeoJSON polygons are only supported if they form a box");
            } else {
                throw new IllegalArgumentException("GeoJSON support is restricted to Point, Circle or Polygon.");
            }
        }

        private double min(double... numbers) {
            return Arrays.stream(numbers).min().getAsDouble();
        }

        private double max(double... numbers) {
            return Arrays.stream(numbers).max().getAsDouble();
        }


        @Override
        public Geoshape read(ScanBuffer buffer) {
            long l = VariableLong.readPositive(buffer);
            assert l>0 && l<Integer.MAX_VALUE;
            int length = (int)l;
            float[][] coordinates = new float[2][];
            for (int i = 0; i < 2; i++) {
                coordinates[i]=buffer.getFloats(length);
            }
            return new Geoshape(coordinates);
        }

        @Override
        public void write(WriteBuffer buffer, Geoshape attribute) {
            float[][] coordinates = attribute.coordinates;
            assert (coordinates.length==2);
            assert (coordinates[0].length==coordinates[1].length && coordinates[0].length>0);
            int length = coordinates[0].length;
            VariableLong.writePositive(buffer,length);
            for (int i = 0; i < 2; i++) {
                for (int j = 0; j < length; j++) {
                    buffer.putFloat(coordinates[i][j]);
                }
            }
        }
    }

    /**
     * Serializer for TinkerPop's Gryo.
     */
    public static class GeoShapeGryoSerializer extends Serializer<Geoshape> {
        @Override
        public void write(Kryo kryo, Output output, Geoshape geoshape) {
            float[][] coordinates = geoshape.coordinates;
            assert (coordinates.length==2);
            assert (coordinates[0].length==coordinates[1].length && coordinates[0].length>0);
            int length = coordinates[0].length;
            output.writeLong(length);
            for (int i = 0; i < 2; i++) {
                for (int j = 0; j < length; j++) {
                    output.writeFloat(coordinates[i][j]);
                }
            }
        }

        @Override
        public Geoshape read(Kryo kryo, Input input, Class<Geoshape> aClass) {
            long l = input.readLong();
            assert l>0 && l<Integer.MAX_VALUE;
            int length = (int)l;
            float[][] coordinates = new float[2][];
            for (int i = 0; i < 2; i++) {
                coordinates[i] = input.readFloats(length);
            }
            return new Geoshape(coordinates);
        }
    }

    /**
     * Serialization of Geoshape for JSON purposes uses the standard GeoJSON(http://geojson.org/) format.
     *
     * @author Bryn Cooke
     */
    public static class GeoshapeGsonSerializer extends StdSerializer<Geoshape> {

        public GeoshapeGsonSerializer() {
            super(Geoshape.class);
        }

        @Override
        public void serialize(Geoshape value, JsonGenerator jgen, SerializerProvider provider) throws IOException, JsonProcessingException {
            jgen.writeStartObject();
            jgen.writeFieldName(FIELD_TYPE);

            switch(value.getType()) {
                case POLYGON:
                    throw new UnsupportedOperationException("Polygons are not supported");
                case BOX:
                    jgen.writeString(Type.BOX.toString());
                    jgen.writeFieldName(FIELD_COORDINATES);
                    jgen.writeStartArray();

                    jgen.writeStartArray();
                    jgen.writeNumber(value.coordinates[1][0]);
                    jgen.writeNumber(value.coordinates[0][0]);
                    jgen.writeEndArray();

                    jgen.writeStartArray();
                    jgen.writeNumber(value.coordinates[1][1]);
                    jgen.writeNumber(value.coordinates[0][0]);
                    jgen.writeEndArray();

                    jgen.writeStartArray();
                    jgen.writeNumber(value.coordinates[1][1]);
                    jgen.writeNumber(value.coordinates[0][1]);
                    jgen.writeEndArray();

                    jgen.writeStartArray();
                    jgen.writeNumber(value.coordinates[1][0]);
                    jgen.writeNumber(value.coordinates[0][1]);
                    jgen.writeEndArray();

                    jgen.writeEndArray();
                    break;
                case CIRCLE:
                    jgen.writeString(Type.CIRCLE.toString());
                    jgen.writeFieldName(FIELD_RADIUS);
                    jgen.writeNumber(value.getRadius());
                    jgen.writeFieldName(FIELD_COORDINATES);
                    jgen.writeStartArray();
                    jgen.writeNumber(value.coordinates[1][0]);
                    jgen.writeNumber(value.coordinates[0][0]);
                    jgen.writeEndArray();
                    break;
                case POINT:
                    jgen.writeString(Type.POINT.toString());
                    jgen.writeFieldName(FIELD_COORDINATES);
                    jgen.writeStartArray();
                    jgen.writeNumber(value.coordinates[1][0]);
                    jgen.writeNumber(value.coordinates[0][0]);
                    jgen.writeEndArray();
                    break;
            }
            jgen.writeEndObject();
        }

        @Override
        public void serializeWithType(Geoshape geoshape, JsonGenerator jgen, SerializerProvider serializerProvider,
                                      TypeSerializer typeSerializer) throws IOException, JsonProcessingException {
            jgen.writeStartObject();
            if (typeSerializer != null) jgen.writeStringField(GraphSONTokens.CLASS, Geoshape.class.getName());
            GraphSONUtil.writeWithType(FIELD_COORDINATES, geoshape.coordinates, jgen, serializerProvider, typeSerializer);
            jgen.writeEndObject();
        }
    }

    public static class GeoshapeGsonDeserializer extends StdDeserializer<Geoshape> {
        public GeoshapeGsonDeserializer() {
            super(Geoshape.class);
        }

        @Override
        public Geoshape deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException, JsonProcessingException {
            // move the parser forward
            jsonParser.nextToken();

            float[][] f = jsonParser.readValueAs(float[][].class);
            jsonParser.nextToken();
            return new Geoshape(f);
        }
    }
}
