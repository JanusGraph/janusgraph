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

import org.janusgraph.core.attribute.GeoshapeHelper;
import org.janusgraph.core.attribute.JtsGeoshapeHelper;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.locationtech.spatial4j.context.jts.JtsSpatialContext;
import org.locationtech.spatial4j.context.jts.JtsSpatialContextFactory;
import org.locationtech.spatial4j.shape.ShapeFactory;
import org.locationtech.spatial4j.shape.jts.JtsShapeFactory;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;

/**
 * Test of JtsGeoshapeHelper.
 *
 * @author David Clement (davidclement90@laposte.net)
 */
public class JtsGeoshapeHelperTest extends GeoshapeHelperTest {

    @Override
    public GeoshapeHelper getHelper() {
        return new JtsGeoshapeHelper();
    }

    @Override
    public boolean supportJts() {
        return true;
    }

    @Override
    public ShapeFactory getShapeFactory() {
        return new JtsShapeFactory((JtsSpatialContext) getHelper().context, (JtsSpatialContextFactory) getHelper().factory);
    }

    @Test
    public void testPolygonIsClosed() {
        if (supportJts()) {
            List<double[]> coordinates = new ArrayList<>();
            coordinates.add(new double[]{1.0,1.0});
            coordinates.add(new double[]{3.0,1.0});
            coordinates.add(new double[]{2.0,4.0});
            coordinates.add(new double[]{1.0,1.0});
            Geoshape polygon = getHelper().polygon(coordinates);

            assertEquals(4, polygon.size());
        }
    }

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testInvalidPolygon() {
        if (supportJts()) {
            List<double[]> coordinates = new ArrayList<>();
            coordinates.add(new double[]{1.0,1.0});
            coordinates.add(new double[]{3.0,1.0});
            coordinates.add(new double[]{2.0,4.0});
            coordinates.add(new double[]{1.0,2.0});
            thrown.expect(IllegalArgumentException.class);
            thrown.expectMessage(equalTo("Polygon is not closed"));
            getHelper().polygon(coordinates);
        }
    }
}
