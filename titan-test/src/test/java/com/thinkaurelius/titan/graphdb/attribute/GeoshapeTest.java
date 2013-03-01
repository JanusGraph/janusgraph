package com.thinkaurelius.titan.graphdb.attribute;

import com.thinkaurelius.titan.core.attribute.Geoshape;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class GeoshapeTest {

    @Test
    public void testDistance() {
        Geoshape p1 = Geoshape.point(37.759,-122.536);
        Geoshape p2 = Geoshape.point(35.714,-105.938);

        double distance = 1496;
        assertEquals(distance,p1.getPoint().distance(p2.getPoint()),5.0);

        p1 = Geoshape.point(0.0,0.0);
        p2 = Geoshape.point(10.0,10.0);
        //System.out.println(p1.getPoint().distance(p2.getPoint()));
    }

    @Test
    public void testIntersection() {
        for (int i=0;i<50;i++) {
            Geoshape point = Geoshape.point(i,i);
            Geoshape circle = Geoshape.circle(0.0,0.0,point.getPoint().distance(Geoshape.point(0,0).getPoint())+10);
            assertTrue(circle.intersect(point));
            assertTrue(point.intersect(circle));
            assertTrue(circle.intersect(circle));
        }
    }

}
