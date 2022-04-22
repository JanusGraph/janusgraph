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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

public abstract class ErrorDistanceCircleProcessorTest {

    protected final ErrorDistanceCircleProcessor processorWithBoundingBoxFallbackAndNormalErrorDistance;
    protected final ErrorDistanceCircleProcessor processorWithoutBoundingBoxFallbackAndNormalErrorDistance;
    protected final ErrorDistanceCircleProcessor processorWithBoundingBoxFallbackAndSmallErrorDistance;
    protected final ErrorDistanceCircleProcessor processorWithoutBoundingBoxFallbackAndSmallErrorDistance;

    public ErrorDistanceCircleProcessorTest(ErrorDistanceCircleProcessor processorWithBoundingBoxFallbackAndNormalErrorDistance,
                                            ErrorDistanceCircleProcessor processorWithoutBoundingBoxFallbackAndNormalErrorDistance,
                                            ErrorDistanceCircleProcessor processorWithBoundingBoxFallbackAndSmallErrorDistance,
                                            ErrorDistanceCircleProcessor processorWithoutBoundingBoxFallbackAndSmallErrorDistance) {
        this.processorWithBoundingBoxFallbackAndNormalErrorDistance = processorWithBoundingBoxFallbackAndNormalErrorDistance;
        this.processorWithoutBoundingBoxFallbackAndNormalErrorDistance = processorWithoutBoundingBoxFallbackAndNormalErrorDistance;
        this.processorWithBoundingBoxFallbackAndSmallErrorDistance = processorWithBoundingBoxFallbackAndSmallErrorDistance;
        this.processorWithoutBoundingBoxFallbackAndSmallErrorDistance = processorWithoutBoundingBoxFallbackAndSmallErrorDistance;
    }

    @Test
    public void testRegularCircleToPolygon() {

        for(Double radiusKM : Arrays.asList(
            0.008993203677616636d,
            0.01d,
            0.02d,
            0.03d,
            0.1d,
            0.2d,
            1d,
            1.1d,
            1.2d,
            2d,
            2.5d,
            3d,
            3.7d,
            10d,
            100d,
            1000d,
            3000d)){
            Geoshape circle = Geoshape.circle(0.0, 0.0, radiusKM);
            Geoshape polygon = processorWithoutBoundingBoxFallbackAndNormalErrorDistance.process(circle);
            Assertions.assertEquals(Geoshape.Type.POLYGON, polygon.getType());
            polygon = processorWithoutBoundingBoxFallbackAndSmallErrorDistance.process(circle);
            Assertions.assertEquals(Geoshape.Type.POLYGON, polygon.getType());
        }
    }

    @Test
    public void testCircleWithPoles() {

        Geoshape northPoleCircle = Geoshape.circle(90, 0, 13330);
        Geoshape southPoleCircle = Geoshape.circle(-90, 0, 13330);

        Geoshape boxFromNorthPoleCircle = processorWithBoundingBoxFallbackAndNormalErrorDistance.process(northPoleCircle);
        Assertions.assertEquals(Geoshape.Type.BOX, boxFromNorthPoleCircle.getType());
        Assertions.assertEquals(northPoleCircle.getShape().getBoundingBox(), boxFromNorthPoleCircle.getShape());

        RuntimeException northPoleException = Assertions.assertThrows(RuntimeException.class, () ->
            processorWithoutBoundingBoxFallbackAndNormalErrorDistance.process(northPoleCircle));
        Assertions.assertTrue(northPoleException.getMessage().contains("contains the north pole"));

        Geoshape boxFromSouthPoleCircle = processorWithBoundingBoxFallbackAndNormalErrorDistance.process(southPoleCircle);
        Assertions.assertEquals(Geoshape.Type.BOX, boxFromSouthPoleCircle.getType());
        Assertions.assertEquals(southPoleCircle.getShape().getBoundingBox(), boxFromSouthPoleCircle.getShape());

        RuntimeException southPoleException = Assertions.assertThrows(RuntimeException.class, () ->
            processorWithoutBoundingBoxFallbackAndNormalErrorDistance.process(southPoleCircle));
        Assertions.assertTrue(southPoleException.getMessage().contains("contains the south pole"));
    }

    @Test
    public void testWholePlanetShape() {

        Geoshape wholeEarthCircle = Geoshape.circle(89.9, 179.7, 20100);
        Geoshape maxRadiusCircleEarthCircle = Geoshape.circle(89.9, 179.7, Integer.MAX_VALUE);

        Geoshape wholePlanetBoundingBox = processorWithBoundingBoxFallbackAndNormalErrorDistance.process(wholeEarthCircle);
        Assertions.assertEquals(Geoshape.Type.BOX, wholePlanetBoundingBox.getType());
        Assertions.assertEquals(wholeEarthCircle.getShape().getBoundingBox(), wholePlanetBoundingBox.getShape());

        wholePlanetBoundingBox = processorWithBoundingBoxFallbackAndNormalErrorDistance.process(maxRadiusCircleEarthCircle);
        Assertions.assertEquals(Geoshape.Type.BOX, wholePlanetBoundingBox.getType());
        Assertions.assertEquals(maxRadiusCircleEarthCircle.getShape().getBoundingBox(), wholePlanetBoundingBox.getShape());

        wholePlanetBoundingBox = processorWithoutBoundingBoxFallbackAndNormalErrorDistance.process(wholeEarthCircle);
        Assertions.assertEquals(Geoshape.Type.BOX, wholePlanetBoundingBox.getType());
        Assertions.assertEquals(wholeEarthCircle.getShape().getBoundingBox(), wholePlanetBoundingBox.getShape());

        wholePlanetBoundingBox = processorWithoutBoundingBoxFallbackAndNormalErrorDistance.process(maxRadiusCircleEarthCircle);
        Assertions.assertEquals(Geoshape.Type.BOX, wholePlanetBoundingBox.getType());
        Assertions.assertEquals(maxRadiusCircleEarthCircle.getShape().getBoundingBox(), wholePlanetBoundingBox.getShape());
    }

    @Test
    public void testSmallRadiusCircle() {

        // Circle with hard to calculate radius which is in bound of error distance can be represented as Point
        Geoshape smallRadiusCircle = Geoshape.circle(-48.0, 0.0, 0.000008993203677616636);
        Geoshape point = processorWithBoundingBoxFallbackAndNormalErrorDistance.process(smallRadiusCircle);
        Assertions.assertEquals(Geoshape.Type.POINT, point.getType());
        Assertions.assertEquals(smallRadiusCircle.getShape().getCenter(), point.getShape());

        Geoshape boundingBox = processorWithBoundingBoxFallbackAndSmallErrorDistance.process(smallRadiusCircle);
        Assertions.assertEquals(Geoshape.Type.BOX, boundingBox.getType());
        Assertions.assertEquals(smallRadiusCircle.getShape().getBoundingBox(), boundingBox.getShape());

        IllegalArgumentException exception = Assertions.assertThrows(IllegalArgumentException.class, () ->
            processorWithoutBoundingBoxFallbackAndSmallErrorDistance.process(smallRadiusCircle));

        Assertions.assertTrue(exception.getMessage().contains("cannot be converted to another shape"));
    }

    @Test
    public void testOnlyCircleShapeAccepted() {

        Geoshape box = Geoshape.box(-48.0, 0.0, 10, 10);
        IllegalArgumentException exception = Assertions.assertThrows(IllegalArgumentException.class,
            () -> processorWithBoundingBoxFallbackAndNormalErrorDistance.process(box));

        Assertions.assertTrue(exception.getMessage().contains("Cannot process non-circle shape"));

        exception = Assertions.assertThrows(IllegalArgumentException.class,
            () -> processorWithoutBoundingBoxFallbackAndNormalErrorDistance.process(box));

        Assertions.assertTrue(exception.getMessage().contains("Cannot process non-circle shape"));
    }
}
