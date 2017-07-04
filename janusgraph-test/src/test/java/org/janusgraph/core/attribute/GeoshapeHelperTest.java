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


import org.janusgraph.core.attribute.Geoshape;
import org.janusgraph.core.attribute.Geoshape.Point;
import org.janusgraph.core.attribute.GeoshapeHelper;
import org.junit.Assert;
import org.junit.Test;
import org.locationtech.spatial4j.shape.Shape;
import org.locationtech.spatial4j.shape.ShapeFactory;
import org.locationtech.spatial4j.shape.impl.ShapeFactoryImpl;

/**
 * Test of GeoshapeHelper.
 *
 * @author David Clement (davidclement90@laposte.net)
 */
public class GeoshapeHelperTest {
    
    public GeoshapeHelper getHelper() {
        return new GeoshapeHelper();
    }

    public boolean supportJts() {
        return false;
    }

    public ShapeFactory getShapeFactory() {
        return new ShapeFactoryImpl(getHelper().context, getHelper().factory);
    }

    @Test
    public void testGetType() {
        Assert.assertEquals(Geoshape.Type.POINT, getHelper().getType(getShapeFactory().pointXY(1.0, 2.0)));
        Assert.assertEquals(Geoshape.Type.CIRCLE, getHelper().getType(getShapeFactory().circle(1.0, 2.0, 200)));
        Assert.assertEquals(Geoshape.Type.LINE, getHelper().getType(getShapeFactory().lineString().pointXY(1.0, 2.0).pointXY(3.0, 4.0).build()));
        Assert.assertEquals(Geoshape.Type.BOX, getHelper().getType(getShapeFactory().rect(-1.0, 1.0, -1.0, 1.0)));
        Assert.assertEquals(Geoshape.Type.MULTIPOINT, getHelper().getType(getShapeFactory().multiPoint().pointXY(60.0, 60.0).pointXY(120.0, 60.0).build()));
        Assert.assertEquals(Geoshape.Type.MULTILINESTRING,  getHelper().getType(getShapeFactory().multiLineString()
                .add(getShapeFactory().lineString().pointXY(59.0, 60.0).pointXY(61.0, 60.0)).add(getShapeFactory().lineString().pointXY(119.0, 60.0).pointXY(121.0, 60.0)).build()));
        Assert.assertEquals(Geoshape.Type.GEOMETRYCOLLECTION,  getHelper().getType(getShapeFactory().multiShape(Shape.class).add(getShapeFactory().pointXY(60.0, 60.0))
                .add(getShapeFactory().lineString().pointXY(119.0, 60.0).pointXY(121.0, 60.0).build()).build()));
        if (supportJts()) {
            Assert.assertEquals(Geoshape.Type.POLYGON, getHelper().getType(getShapeFactory().polygon().pointXY(1.0, 2.0).pointXY(3.0, 4.0).pointXY(3.0, -4.0).pointXY(1.0, -4.0).pointXY(1.0, 2.0).build()));
            Assert.assertEquals(Geoshape.Type.MULTIPOLYGON, getHelper().getType(getShapeFactory().multiPolygon()
                    .add(Geoshape.getShapeFactory().polygon().pointXY(59.0, 59.0).pointXY(61.0, 59.0).pointXY(61.0, 61.0).pointXY(59.0, 61.0).pointXY(59.0, 59.0))
                    .add(Geoshape.getShapeFactory().polygon().pointXY(119.0, 59.0).pointXY(121.0, 59.0).pointXY(121.0, 61.0).pointXY(119.0, 61.0).pointXY(119.0, 59.0)).build()));
            Assert.assertEquals(Geoshape.Type.GEOMETRYCOLLECTION,  getHelper().getType(getShapeFactory().multiShape(Shape.class).add(getShapeFactory().pointXY(60.0, 60.0))
                    .add(getShapeFactory().lineString().pointXY(119.0, 60.0).pointXY(121.0, 60.0).build())
                    .add(getShapeFactory().polygon().pointXY(119.0, 59.0).pointXY(121.0, 59.0).pointXY(121.0, 61.0).pointXY(119.0, 61.0).pointXY(119.0, 59.0).build()).build()));
        }
    }

    @Test
    public void testSize() {
        Assert.assertEquals(1, getHelper().size(getShapeFactory().pointXY(1.0, 2.0)));
        Assert.assertEquals(1, getHelper().size(getShapeFactory().circle(1.0, 2.0, 200)));
        Assert.assertEquals(2, getHelper().size(getShapeFactory().lineString().pointXY(1.0, 2.0).pointXY(3.0, 4.0).build()));
        Assert.assertEquals(2, getHelper().size(getShapeFactory().rect(-1.0, 1.0, -1.0, 1.0)));
        Assert.assertEquals(2, getHelper().size(getShapeFactory().multiPoint().pointXY(60.0, 60.0).pointXY(120.0, 60.0).build()));
        Assert.assertEquals(4, getHelper().size(getShapeFactory().multiLineString()
                .add(getShapeFactory().lineString().pointXY(59.0, 60.0).pointXY(61.0, 60.0)).add(getShapeFactory().lineString().pointXY(119.0, 60.0).pointXY(121.0, 60.0)).build()));
        Assert.assertEquals(3,  getHelper().size(getShapeFactory().multiShape(Shape.class).add(getShapeFactory().pointXY(60.0, 60.0))
                .add(getShapeFactory().lineString().pointXY(119.0, 60.0).pointXY(121.0, 60.0).build()).build()));
        if (supportJts()) {
            Assert.assertEquals(5, getHelper().size(getShapeFactory().polygon().pointXY(1.0, 2.0).pointXY(3.0, 4.0).pointXY(3.0, -4.0).pointXY(1.0, -4.0).pointXY(1.0, 2.0).build()));
            Assert.assertEquals(10, getHelper().size(getShapeFactory().multiPolygon()
                    .add(Geoshape.getShapeFactory().polygon().pointXY(59.0, 59.0).pointXY(61.0, 59.0).pointXY(61.0, 61.0).pointXY(59.0, 61.0).pointXY(59.0, 59.0))
                    .add(Geoshape.getShapeFactory().polygon().pointXY(119.0, 59.0).pointXY(121.0, 59.0).pointXY(121.0, 61.0).pointXY(119.0, 61.0).pointXY(119.0, 59.0)).build()));
            Assert.assertEquals(8,  getHelper().size(getShapeFactory().multiShape(Shape.class).add(getShapeFactory().pointXY(60.0, 60.0))
                    .add(getShapeFactory().lineString().pointXY(119.0, 60.0).pointXY(121.0, 60.0).build())
                    .add(getShapeFactory().polygon().pointXY(119.0, 59.0).pointXY(121.0, 59.0).pointXY(121.0, 61.0).pointXY(119.0, 61.0).pointXY(119.0, 59.0).build()).build()));
        }
    }

    @Test
    public void testGetPoint() {
        Geoshape shape = new Geoshape(getShapeFactory().pointXY(1.0, 2.0));
        Point point = getHelper().getPoint(shape, 0);
        Assert.assertEquals(1.0, point.getLongitude(), 0.0);
        Assert.assertEquals(2.0, point.getLatitude(), 0.0);
        shape = new Geoshape(getShapeFactory().circle(1.0, 2.0, 200));
        point = getHelper().getPoint(shape, 0);
        Assert.assertEquals(1.0, point.getLongitude(), 0.0);
        Assert.assertEquals(2.0, point.getLatitude(), 0.0);
        shape = new Geoshape(getShapeFactory().lineString().pointXY(1.0, 2.0).pointXY(3.0, 4.0).build());
        point = getHelper().getPoint(shape, 0);
        Assert.assertEquals(1.0, point.getLongitude(), 0.0);
        Assert.assertEquals(2.0, point.getLatitude(), 0.0);
        point = getHelper().getPoint(shape, 1);
        Assert.assertEquals(3.0, point.getLongitude(), 0.0);
        Assert.assertEquals(4.0, point.getLatitude(), 0.0);
        shape = new Geoshape(getShapeFactory().rect(-1.0, 2.0, -3.0, 4.0));
        point = getHelper().getPoint(shape, 0);
        Assert.assertEquals(-1.0, point.getLongitude(), 0.0);
        Assert.assertEquals(-3.0, point.getLatitude(), 0.0);
        point = getHelper().getPoint(shape, 1);
        Assert.assertEquals(2.0, point.getLongitude(), 0.0);
        Assert.assertEquals(4.0, point.getLatitude(), 0.0);
        shape = new Geoshape(getShapeFactory().multiPoint().pointXY(60.0, 90.0).pointXY(120.0, 60.0).build());
        point = getHelper().getPoint(shape, 0);
        Assert.assertEquals(60, point.getLongitude(), 0.0);
        Assert.assertEquals(90, point.getLatitude(), 0.0);
        point = getHelper().getPoint(shape, 1);
        Assert.assertEquals(120, point.getLongitude(), 0.0);
        Assert.assertEquals(60, point.getLatitude(), 0.0);
        shape = new Geoshape(getShapeFactory().multiLineString().add(getShapeFactory().lineString().pointXY(59.0, 60.0).pointXY(61.0, 60.0)).add(getShapeFactory().lineString().pointXY(119.0, 60.0).pointXY(121.0, 60.0)).build());
        point = getHelper().getPoint(shape, 0);
        Assert.assertEquals(59, point.getLongitude(), 0.0);
        Assert.assertEquals(60, point.getLatitude(), 0.0);
        point = getHelper().getPoint(shape, 1);
        Assert.assertEquals(61, point.getLongitude(), 0.0);
        Assert.assertEquals(60, point.getLatitude(), 0.0);
        point = getHelper().getPoint(shape, 2);
        Assert.assertEquals(119, point.getLongitude(), 0.0);
        Assert.assertEquals(60, point.getLatitude(), 0.0);
        point = getHelper().getPoint(shape, 3);
        Assert.assertEquals(121, point.getLongitude(), 0.0);
        Assert.assertEquals(60, point.getLatitude(), 0.0);
        shape = new Geoshape(getShapeFactory().multiShape(Shape.class).add(getShapeFactory().pointXY(59.0, 60.0)).add(getShapeFactory().lineString().pointXY(119.0, 60.0).pointXY(121.0, 60.0).build()).build());
        point = getHelper().getPoint(shape, 0);
        Assert.assertEquals(59, point.getLongitude(), 0.0);
        Assert.assertEquals(60, point.getLatitude(), 0.0);
        point = getHelper().getPoint(shape, 1);
        Assert.assertEquals(119, point.getLongitude(), 0.0);
        Assert.assertEquals(60, point.getLatitude(), 0.0);
        point = getHelper().getPoint(shape, 2);
        Assert.assertEquals(121, point.getLongitude(), 0.0);
        Assert.assertEquals(60, point.getLatitude(), 0.0);
        if (supportJts()) {
            shape = new Geoshape(getShapeFactory().polygon().pointXY(1.0, 2.0).pointXY(3.0, 4.0).pointXY(3.0, -4.0).pointXY(1.0, -4.0).pointXY(1.0, 2.0).build());
            point = getHelper().getPoint(shape, 0);
            Assert.assertEquals(1.0, point.getLongitude(), 0.0);
            Assert.assertEquals(2.0, point.getLatitude(), 0.0);
            point = getHelper().getPoint(shape, 1);
            Assert.assertEquals(3.0, point.getLongitude(), 0.0);
            Assert.assertEquals(4.0, point.getLatitude(), 0.0);
            point = getHelper().getPoint(shape, 2);
            Assert.assertEquals(3.0, point.getLongitude(), 0.0);
            Assert.assertEquals(-4.0, point.getLatitude(), 0.0);
            point = getHelper().getPoint(shape, 3);
            Assert.assertEquals(1.0, point.getLongitude(), 0.0);
            Assert.assertEquals(-4.0, point.getLatitude(), 0.0);
            point = getHelper().getPoint(shape, 4);
            Assert.assertEquals(1.0, point.getLongitude(), 0.0);
            Assert.assertEquals(2.0, point.getLatitude(), 0.0);
            shape = new Geoshape(getShapeFactory().multiPolygon().add(Geoshape.getShapeFactory().polygon().pointXY(59.0, 59.0).pointXY(61.0, 59.0).pointXY(61.0, 61.0).pointXY(59.0, 61.0).pointXY(59.0, 59.0))
                    .add(Geoshape.getShapeFactory().polygon().pointXY(119.0, 59.0).pointXY(121.0, 59.0).pointXY(121.0, 61.0).pointXY(119.0, 61.0).pointXY(119.0, 59.0)).build());
            point = getHelper().getPoint(shape, 2);
            Assert.assertEquals(61.0, point.getLongitude(), 0.0);
            Assert.assertEquals(61.0, point.getLatitude(), 0.0);
            point = getHelper().getPoint(shape, 7);
            Assert.assertEquals(121.0, point.getLongitude(), 0.0);
            Assert.assertEquals(61.0, point.getLatitude(), 0.0);
            shape = new Geoshape(getShapeFactory().multiShape(Shape.class).add(getShapeFactory().pointXY(60.0, 60.0))
                    .add(getShapeFactory().lineString().pointXY(119.0, 60.0).pointXY(121.0, 60.0).build())
                    .add(getShapeFactory().polygon().pointXY(119.0, 59.0).pointXY(121.0, 59.0).pointXY(121.0, 61.0).pointXY(119.0, 61.0).pointXY(119.0, 59.0).build()).build());
            point = getHelper().getPoint(shape, 0);
            Assert.assertEquals(60.0, point.getLongitude(), 0.0);
            Assert.assertEquals(60.0, point.getLatitude(), 0.0);
            point = getHelper().getPoint(shape, 2);
            Assert.assertEquals(121.0, point.getLongitude(), 0.0);
            Assert.assertEquals(60.0, point.getLatitude(), 0.0);
            point = getHelper().getPoint(shape, 6);
            Assert.assertEquals(119.0, point.getLongitude(), 0.0);
            Assert.assertEquals(61.0, point.getLatitude(), 0.0);
        }
    }
}
