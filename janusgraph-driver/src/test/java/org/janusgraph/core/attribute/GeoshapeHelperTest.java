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

import org.janusgraph.core.attribute.Geoshape.Point;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.locationtech.spatial4j.shape.Shape;
import org.locationtech.spatial4j.shape.ShapeFactory;
import org.locationtech.spatial4j.shape.jts.JtsShapeFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test of GeoshapeHelper.
 *
 * @author David Clement (davidclement90@laposte.net)
 */
public class GeoshapeHelperTest {

    private JtsGeoshapeHelper helper;

    private ShapeFactory factory;

    @BeforeEach
    public void setUp() {
        this.helper = new JtsGeoshapeHelper();
        this.factory = new JtsShapeFactory(helper.context, helper.factory);
    }

    @Test
    public void testGetType() {
        assertEquals(Geoshape.Type.POINT, helper.getType(factory.pointXY(1.0, 2.0)));
        assertEquals(Geoshape.Type.CIRCLE, helper.getType(factory.circle(1.0, 2.0, 200)));
        assertEquals(Geoshape.Type.LINE, helper.getType(factory.lineString().pointXY(1.0, 2.0).pointXY(3.0, 4.0).build()));
        assertEquals(Geoshape.Type.BOX, helper.getType(factory.rect(-1.0, 1.0, -1.0, 1.0)));
        assertEquals(Geoshape.Type.MULTIPOINT, helper.getType(factory.multiPoint().pointXY(60.0, 60.0).pointXY(120.0, 60.0).build()));
        assertEquals(Geoshape.Type.MULTILINESTRING,  helper.getType(factory.multiLineString()
                .add(factory.lineString().pointXY(59.0, 60.0).pointXY(61.0, 60.0)).add(factory.lineString().pointXY(119.0, 60.0).pointXY(121.0, 60.0)).build()));
        assertEquals(Geoshape.Type.GEOMETRYCOLLECTION,  helper.getType(factory.multiShape(Shape.class).add(factory.pointXY(60.0, 60.0))
                .add(factory.lineString().pointXY(119.0, 60.0).pointXY(121.0, 60.0).build()).build()));
        assertEquals(Geoshape.Type.POLYGON, helper.getType(factory.polygon().pointXY(1.0, 2.0).pointXY(3.0, 4.0).pointXY(3.0, -4.0).pointXY(1.0, -4.0).pointXY(1.0, 2.0).build()));
        assertEquals(Geoshape.Type.MULTIPOLYGON, helper.getType(factory.multiPolygon()
            .add(Geoshape.getShapeFactory().polygon().pointXY(59.0, 59.0).pointXY(61.0, 59.0).pointXY(61.0, 61.0).pointXY(59.0, 61.0).pointXY(59.0, 59.0))
            .add(Geoshape.getShapeFactory().polygon().pointXY(119.0, 59.0).pointXY(121.0, 59.0).pointXY(121.0, 61.0).pointXY(119.0, 61.0).pointXY(119.0, 59.0)).build()));
        assertEquals(Geoshape.Type.GEOMETRYCOLLECTION,  helper.getType(factory.multiShape(Shape.class).add(factory.pointXY(60.0, 60.0))
            .add(factory.lineString().pointXY(119.0, 60.0).pointXY(121.0, 60.0).build())
            .add(factory.polygon().pointXY(119.0, 59.0).pointXY(121.0, 59.0).pointXY(121.0, 61.0).pointXY(119.0, 61.0).pointXY(119.0, 59.0).build()).build()));
    }

    @Test
    public void testSize() {
        assertEquals(1, helper.size(factory.pointXY(1.0, 2.0)));
        assertEquals(1, helper.size(factory.circle(1.0, 2.0, 200)));
        assertEquals(2, helper.size(factory.lineString().pointXY(1.0, 2.0).pointXY(3.0, 4.0).build()));
        assertEquals(2, helper.size(factory.rect(-1.0, 1.0, -1.0, 1.0)));
        assertEquals(2, helper.size(factory.multiPoint().pointXY(60.0, 60.0).pointXY(120.0, 60.0).build()));
        assertEquals(4, helper.size(factory.multiLineString()
                .add(factory.lineString().pointXY(59.0, 60.0).pointXY(61.0, 60.0)).add(factory.lineString().pointXY(119.0, 60.0).pointXY(121.0, 60.0)).build()));
        assertEquals(3,  helper.size(factory.multiShape(Shape.class).add(factory.pointXY(60.0, 60.0))
                .add(factory.lineString().pointXY(119.0, 60.0).pointXY(121.0, 60.0).build()).build()));
        assertEquals(5, helper.size(factory.polygon().pointXY(1.0, 2.0).pointXY(3.0, 4.0).pointXY(3.0, -4.0).pointXY(1.0, -4.0).pointXY(1.0, 2.0).build()));
        assertEquals(10, helper.size(factory.multiPolygon()
            .add(Geoshape.getShapeFactory().polygon().pointXY(59.0, 59.0).pointXY(61.0, 59.0).pointXY(61.0, 61.0).pointXY(59.0, 61.0).pointXY(59.0, 59.0))
            .add(Geoshape.getShapeFactory().polygon().pointXY(119.0, 59.0).pointXY(121.0, 59.0).pointXY(121.0, 61.0).pointXY(119.0, 61.0).pointXY(119.0, 59.0)).build()));
        assertEquals(8,  helper.size(factory.multiShape(Shape.class).add(factory.pointXY(60.0, 60.0))
            .add(factory.lineString().pointXY(119.0, 60.0).pointXY(121.0, 60.0).build())
            .add(factory.polygon().pointXY(119.0, 59.0).pointXY(121.0, 59.0).pointXY(121.0, 61.0).pointXY(119.0, 61.0).pointXY(119.0, 59.0).build()).build()));
    }

    @Test
    public void testGetPoint() {
        Geoshape shape = new Geoshape(factory.pointXY(1.0, 2.0));
        Point point = helper.getPoint(shape, 0);
        assertEquals(1.0, point.getLongitude(), 0.0);
        assertEquals(2.0, point.getLatitude(), 0.0);
        shape = new Geoshape(factory.circle(1.0, 2.0, 200));
        point = helper.getPoint(shape, 0);
        assertEquals(1.0, point.getLongitude(), 0.0);
        assertEquals(2.0, point.getLatitude(), 0.0);
        shape = new Geoshape(factory.lineString().pointXY(1.0, 2.0).pointXY(3.0, 4.0).build());
        point = helper.getPoint(shape, 0);
        assertEquals(1.0, point.getLongitude(), 0.0);
        assertEquals(2.0, point.getLatitude(), 0.0);
        point = helper.getPoint(shape, 1);
        assertEquals(3.0, point.getLongitude(), 0.0);
        assertEquals(4.0, point.getLatitude(), 0.0);
        shape = new Geoshape(factory.rect(-1.0, 2.0, -3.0, 4.0));
        point = helper.getPoint(shape, 0);
        assertEquals(-1.0, point.getLongitude(), 0.0);
        assertEquals(-3.0, point.getLatitude(), 0.0);
        point = helper.getPoint(shape, 1);
        assertEquals(2.0, point.getLongitude(), 0.0);
        assertEquals(4.0, point.getLatitude(), 0.0);
        shape = new Geoshape(factory.multiPoint().pointXY(60.0, 90.0).pointXY(120.0, 60.0).build());
        point = helper.getPoint(shape, 0);
        assertEquals(60, point.getLongitude(), 0.0);
        assertEquals(90, point.getLatitude(), 0.0);
        point = helper.getPoint(shape, 1);
        assertEquals(120, point.getLongitude(), 0.0);
        assertEquals(60, point.getLatitude(), 0.0);
        shape = new Geoshape(factory.multiLineString().add(factory.lineString().pointXY(59.0, 60.0).pointXY(61.0, 60.0)).add(factory.lineString().pointXY(119.0, 60.0).pointXY(121.0, 60.0)).build());
        point = helper.getPoint(shape, 0);
        assertEquals(59, point.getLongitude(), 0.0);
        assertEquals(60, point.getLatitude(), 0.0);
        point = helper.getPoint(shape, 1);
        assertEquals(61, point.getLongitude(), 0.0);
        assertEquals(60, point.getLatitude(), 0.0);
        point = helper.getPoint(shape, 2);
        assertEquals(119, point.getLongitude(), 0.0);
        assertEquals(60, point.getLatitude(), 0.0);
        point = helper.getPoint(shape, 3);
        assertEquals(121, point.getLongitude(), 0.0);
        assertEquals(60, point.getLatitude(), 0.0);
        shape = new Geoshape(factory.multiShape(Shape.class).add(factory.pointXY(59.0, 60.0)).add(factory.lineString().pointXY(119.0, 60.0).pointXY(121.0, 60.0).build()).build());
        point = helper.getPoint(shape, 0);
        assertEquals(59, point.getLongitude(), 0.0);
        assertEquals(60, point.getLatitude(), 0.0);
        point = helper.getPoint(shape, 1);
        assertEquals(119, point.getLongitude(), 0.0);
        assertEquals(60, point.getLatitude(), 0.0);
        point = helper.getPoint(shape, 2);
        assertEquals(121, point.getLongitude(), 0.0);
        assertEquals(60, point.getLatitude(), 0.0);
        shape = new Geoshape(factory.polygon().pointXY(1.0, 2.0).pointXY(3.0, 4.0).pointXY(3.0, -4.0).pointXY(1.0, -4.0).pointXY(1.0, 2.0).build());
        point = helper.getPoint(shape, 0);
        assertEquals(1.0, point.getLongitude(), 0.0);
        assertEquals(2.0, point.getLatitude(), 0.0);
        point = helper.getPoint(shape, 1);
        assertEquals(3.0, point.getLongitude(), 0.0);
        assertEquals(4.0, point.getLatitude(), 0.0);
        point = helper.getPoint(shape, 2);
        assertEquals(3.0, point.getLongitude(), 0.0);
        assertEquals(-4.0, point.getLatitude(), 0.0);
        point = helper.getPoint(shape, 3);
        assertEquals(1.0, point.getLongitude(), 0.0);
        assertEquals(-4.0, point.getLatitude(), 0.0);
        point = helper.getPoint(shape, 4);
        assertEquals(1.0, point.getLongitude(), 0.0);
        assertEquals(2.0, point.getLatitude(), 0.0);
        shape = new Geoshape(factory.multiPolygon().add(Geoshape.getShapeFactory().polygon().pointXY(59.0, 59.0).pointXY(61.0, 59.0).pointXY(61.0, 61.0).pointXY(59.0, 61.0).pointXY(59.0, 59.0))
            .add(Geoshape.getShapeFactory().polygon().pointXY(119.0, 59.0).pointXY(121.0, 59.0).pointXY(121.0, 61.0).pointXY(119.0, 61.0).pointXY(119.0, 59.0)).build());
        point = helper.getPoint(shape, 2);
        assertEquals(61.0, point.getLongitude(), 0.0);
        assertEquals(61.0, point.getLatitude(), 0.0);
        point = helper.getPoint(shape, 7);
        assertEquals(121.0, point.getLongitude(), 0.0);
        assertEquals(61.0, point.getLatitude(), 0.0);
        shape = new Geoshape(factory.multiShape(Shape.class).add(factory.pointXY(60.0, 60.0))
            .add(factory.lineString().pointXY(119.0, 60.0).pointXY(121.0, 60.0).build())
            .add(factory.polygon().pointXY(119.0, 59.0).pointXY(121.0, 59.0).pointXY(121.0, 61.0).pointXY(119.0, 61.0).pointXY(119.0, 59.0).build()).build());
        point = helper.getPoint(shape, 0);
        assertEquals(60.0, point.getLongitude(), 0.0);
        assertEquals(60.0, point.getLatitude(), 0.0);
        point = helper.getPoint(shape, 2);
        assertEquals(121.0, point.getLongitude(), 0.0);
        assertEquals(60.0, point.getLatitude(), 0.0);
        point = helper.getPoint(shape, 6);
        assertEquals(119.0, point.getLongitude(), 0.0);
        assertEquals(61.0, point.getLatitude(), 0.0);
    }
}
