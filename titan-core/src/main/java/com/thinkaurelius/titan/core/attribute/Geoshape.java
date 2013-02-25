package com.thinkaurelius.titan.core.attribute;

import com.google.common.base.Preconditions;
import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.shape.Shape;
import com.spatial4j.core.shape.SpatialRelation;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class Geoshape {

    private static final SpatialContext CTX = SpatialContext.GEO;

    public enum Type {
        POINT, BOX, CIRCLE, POLYGON;
    }

    //coordinates[0] = latitudes, coordinates[1] = longitudes
    private final float[][] coordinates;

    private Geoshape(final float[][] coordinates) {
        Preconditions.checkArgument(coordinates!=null && coordinates.length==2);
        Preconditions.checkArgument(coordinates[0].length==coordinates[1].length && coordinates[0].length>0);
        for (int i=0;i<coordinates[0].length;i++) {
            if (Float.isNaN(coordinates[0][i])) Preconditions.checkArgument(i==1 && coordinates.length==2 && coordinates[1][i]>0);
            else Preconditions.checkArgument(isValidCoordinate(coordinates[0][i],coordinates[1][i]));
        }
        this.coordinates=coordinates;
    }

    public Type getType() {
        if (coordinates[0].length==1) return Type.POINT;
        else if (coordinates[0].length>2) return Type.POLYGON;
        else { //coordinates[0].length==2
            if (Float.isNaN(coordinates[0][1])) return Type.CIRCLE;
            else return Type.BOX;
        }
    }

    public int size() {
        switch(getType()) {
            case POINT: return 1;
            case CIRCLE: return 1;
            case BOX: return 2;
            case POLYGON: return coordinates[0].length;
            default: throw new IllegalStateException("Unrecognized type: " + getType());
        }
    }

    public Point getPoint(int position) {
        if (position<0 || position>=size()) throw new ArrayIndexOutOfBoundsException("Invalid position: " + position);
        return new Point(coordinates[0][position],coordinates[1][position]);
    }

    public Point getPoint() {
        Preconditions.checkArgument(size()==1,"Shape does not have a single point");
        return getPoint(0);
    }

    public float getRadius() {
        Preconditions.checkArgument(getType()==Type.CIRCLE,"This shape is not a circle");
        return coordinates[1][1];
    }

    private SpatialRelation getSpatialRelation(Geoshape other) {
        Preconditions.checkNotNull(other);
        return convert().relate(other.convert());
    }

    public boolean intersect(Geoshape other) {
        return getSpatialRelation(other)==SpatialRelation.INTERSECTS;
    }

    public boolean within(Geoshape outer) {
        return getSpatialRelation(outer)==SpatialRelation.WITHIN;
    }

    public boolean disjoint(Geoshape other) {
        return getSpatialRelation(other)==SpatialRelation.DISJOINT;
    }

    private Shape convert() {
        switch(getType()) {
            case POINT: return getPoint().getSpatial4jPoint();
            case CIRCLE: return CTX.makeCircle(getPoint(0).getSpatial4jPoint(), getRadius());
            case BOX: return CTX.makeRectangle(getPoint(0).getSpatial4jPoint(),getPoint(1).getSpatial4jPoint());
            case POLYGON: throw new UnsupportedOperationException("Not yet supported");
            default: throw new IllegalStateException("Unrecognized type: " + getType());
        }
    }


    public static final Geoshape point(final float latitude, final float longitude) {
        Preconditions.checkArgument(isValidCoordinate(latitude,longitude),"Invalid coordinate provided");
        return new Geoshape(new float[][]{ new float[]{latitude}, new float[]{longitude}});
    }

    public static final Geoshape circle(final float latitude, final float longitude, final float radiusInDegree) {
        Preconditions.checkArgument(isValidCoordinate(latitude,longitude),"Invalid coordinate provided");
        Preconditions.checkArgument(radiusInDegree>0 && radiusInDegree<=180,"Invalid radius provided [%s]",radiusInDegree);
        return new Geoshape(new float[][]{ new float[]{latitude, Float.NaN}, new float[]{longitude, radiusInDegree}});
    }

    public static final Geoshape box(final float southWestLatitude, final float southWestLongitude,
                                     final float northEastLatitude, final float northEastLongitude) {
        Preconditions.checkArgument(isValidCoordinate(southWestLatitude,southWestLongitude),"Invalid south-west coordinate provided");
        Preconditions.checkArgument(isValidCoordinate(northEastLatitude,northEastLongitude),"Invalid north-east coordinate provided");
        return new Geoshape(new float[][]{ new float[]{southWestLatitude, northEastLatitude}, new float[]{southWestLongitude, northEastLongitude}});
    }

    public static final boolean isValidCoordinate(final float latitude, final float longitude) {
        return latitude>=-90.0 && latitude<=90.0 && longitude>=-180.0 && longitude<=180.0;
    }

    public static final class Point {

        private final float longitude;
        private final float latitude;

        public Point(float latitude, float longitude) {
            this.longitude = longitude;
            this.latitude = latitude;
        }

        public float getLongitude() {
            return longitude;
        }

        public float getLatitude() {
            return latitude;
        }

        private com.spatial4j.core.shape.Point getSpatial4jPoint() {
            return CTX.makePoint(longitude,latitude);
        }

        public double distance(Point other) {
            return CTX.getDistCalc().distance(getSpatial4jPoint(),other.getSpatial4jPoint());
        }
    }

}
