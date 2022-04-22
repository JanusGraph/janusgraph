// Copyright 2023 JanusGraph Authors
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

package org.janusgraph.diskstorage.mixed.utils.processor;

import org.janusgraph.core.attribute.Geoshape;
import org.janusgraph.diskstorage.mixed.utils.CircleUtils;
import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.shape.Circle;
import org.locationtech.spatial4j.shape.Point;
import org.locationtech.spatial4j.shape.Rectangle;
import org.locationtech.spatial4j.shape.Shape;
import org.locationtech.spatial4j.shape.ShapeFactory;
import org.locationtech.spatial4j.shape.impl.PointImpl;

import java.util.HashSet;
import java.util.Set;

public abstract class ErrorDistanceCircleProcessor implements CircleProcessor {

    private final boolean boundingBoxFallback;

    public ErrorDistanceCircleProcessor(boolean boundingBoxFallback){
        this.boundingBoxFallback = boundingBoxFallback;
    }

    abstract double getErrorDistanceMeters(Geoshape circle);

    @Override
    public Geoshape process(Geoshape circle) {
        Shape shape = circle.getShape();
        if(!(shape instanceof Circle)){
            throw new IllegalArgumentException("Cannot process non-circle shape but received "+shape.getClass().getName()
                +". Shape: "+circle.toGeoJson());
        }

        SpatialContext ctx = shape.getContext();

        Circle circleShape = (Circle) shape;

        double circleDegree = circleShape.getRadius();
        if (circleDegree >= 180d){
            // Circle radius is covering the whole Earth. Thus, returning enclosing rectangle
            // (-90,-180,90,180) to cover the whole earth.
            return Geoshape.geoshape(circleShape.getBoundingBox());
        }

        double radiusMeters = circle.getRadiusMeters();
        double errorDistanceMeters = getErrorDistanceMeters(circle);
        int numSides = CircleUtils.circleToPolygonNumSides(radiusMeters, errorDistanceMeters);

        Point center = circleShape.getCenter();

        Shape fallbackShape = fallbackShape(errorDistanceMeters, radiusMeters, boundingBoxFallback, center, circleShape.getBoundingBox());
        Shape resultShape;

        try{
            double[][] rawPolygon = CircleUtils.createRegularGeoShapePolygon(center.getLat(), center.getLon(), radiusMeters, numSides);
            int duplicatePoints = 0;
            final ShapeFactory.PolygonBuilder builder = ctx.getShapeFactory().polygon();

            Set<Point> addedPoints = new HashSet<>(rawPolygon[0].length);
            for(int i=0; i<rawPolygon[0].length; i++){
                Point point = new PointImpl(rawPolygon[0][i], rawPolygon[1][i], ctx);
                if(!addedPoints.add(point)){
                    ++duplicatePoints;
                }
                builder.pointXY(rawPolygon[0][i], rawPolygon[1][i]);
            }

            if(duplicatePoints<2){
                resultShape = builder.build();
            } else {
                if(fallbackShape == null){
                    throw new IllegalArgumentException("Circle: "+circle.toString()+" cannot be converted to another shape " +
                        "following the specified error distance. The Polygon conversion logic produces "+duplicatePoints+" points " +
                        "which is most likely related to either small radius or increased math complexity for Polygon creation.");
                }
                resultShape = fallbackShape;
            }
        } catch (RuntimeException e){
            if(fallbackShape == null){
                if(e instanceof IllegalArgumentException){
                    throw e;
                }
                throw new IllegalArgumentException("Circle: "+circle.toString()+" cannot be converted to another shape " +
                    "following the specified error distance. Reason: "+e.getMessage(), e);
            }
            resultShape = fallbackShape;
        }

        return Geoshape.geoshape(resultShape);
    }

    private Shape fallbackShape(double errorDistanceMeters, double radiusMeters, boolean boundingBoxFallback,
                                Point center, Rectangle boundingBox){
        if(errorDistanceMeters >= radiusMeters){
            return center;
        } else if(boundingBoxFallback){
            return boundingBox;
        }
        return null;
    }
}
