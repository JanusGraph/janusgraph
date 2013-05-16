package com.thinkaurelius.titan.core.attribute;

import com.google.common.base.Preconditions;
import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.distance.DistanceUtils;
import com.spatial4j.core.shape.Shape;
import com.spatial4j.core.shape.SpatialRelation;
import org.apache.commons.lang.builder.HashCodeBuilder;

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

    private static final SpatialContext CTX = SpatialContext.GEO;

    /**
     * The Type of a shape: a point, box, circle, or polygon
     */
    public enum Type {
        POINT, BOX, CIRCLE, POLYGON;
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

}
