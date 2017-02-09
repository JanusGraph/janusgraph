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

package org.janusgraph.diskstorage.solr.transform;

import org.janusgraph.core.attribute.Geoshape;
import org.janusgraph.diskstorage.BackendException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author Jared Holmberg (jholmberg@bericotechnologies.com)
 */
public class GeoToWktConverterTest {

    /**
     * JanusGraph Geoshapes are converted to a string that gets sent to its respective index. Unfortunately, the string format
     * is not compatible with Solr 4. The GeoToWktConverter transforms the Geoshape's string value into a Well-Known Text
     * format understood by Solr.
     */
    @Test
    public void testConvertGeoshapePointToWktString() throws BackendException {
        Geoshape p1 = Geoshape.point(35.4, 48.9); //no spaces, no negative values
        Geoshape p2 = Geoshape.point(-35.4,48.9); //negative longitude value
        Geoshape p3 = Geoshape.point(35.4, -48.9); //negative latitude value

        String wkt1 = "POINT(48.9 35.4)";
        String actualWkt1 = GeoToWktConverter.convertToWktString(p1);

        String wkt2 = "POINT(48.9 -35.4)";
        String actualWkt2 = GeoToWktConverter.convertToWktString(p2);

        String wkt3 = "POINT(-48.9 35.4)";
        String actualWkt3 = GeoToWktConverter.convertToWktString(p3);

        assertEquals(wkt1, actualWkt1);
        assertEquals(wkt2, actualWkt2);
        assertEquals(wkt3, actualWkt3);

    }
}
