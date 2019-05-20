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
import org.locationtech.spatial4j.context.jts.JtsSpatialContext;
import org.locationtech.spatial4j.context.jts.JtsSpatialContextFactory;
import org.locationtech.spatial4j.shape.ShapeFactory;
import org.locationtech.spatial4j.shape.jts.JtsShapeFactory;

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
}
