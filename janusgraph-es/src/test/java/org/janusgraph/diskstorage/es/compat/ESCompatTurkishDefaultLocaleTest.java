// Copyright 2026 JanusGraph Authors
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

package org.janusgraph.diskstorage.es.compat;

import org.janusgraph.core.attribute.Geo;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;

import java.util.Collections;
import java.util.Locale;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * The relation of a geo shape query is named in the root locale. In a Turkish one, WITHIN lower-cases to wıthın, with
 * dotless i's, which Elasticsearch rejects.
 */
@ResourceLock(Resources.LOCALE)
public class ESCompatTurkishDefaultLocaleTest {

    private Locale defaultLocale;

    @BeforeEach
    public void useTurkish() {
        defaultLocale = Locale.getDefault();
        Locale.setDefault(Locale.forLanguageTag("tr-TR"));
    }

    @AfterEach
    public void restoreTheDefaultLocale() {
        Locale.setDefault(defaultLocale);
    }

    @Test
    public void aGeoShapeQueryNamesItsRelationInTheRootLocale() {
        final ES9Compat compat = new ES9Compat();
        assertEquals("within", relation(compat.geoShape("location", Collections.emptyMap(), Geo.WITHIN)));
        assertEquals("disjoint", relation(compat.geoShape("location", Collections.emptyMap(), Geo.DISJOINT)));
        assertEquals("contains", relation(compat.geoShape("location", Collections.emptyMap(), Geo.CONTAINS)));
        assertEquals("intersects", relation(compat.geoShape("location", Collections.emptyMap(), Geo.INTERSECT)));
    }

    // The query is a filter around the geo_shape clause, which names the relation next to the shape
    @SuppressWarnings("unchecked")
    private static Object relation(Object node) {
        if (node instanceof Map) {
            final Map<String, Object> map = (Map<String, Object>) node;
            if (map.containsKey("relation")) {
                return map.get("relation");
            }
            for (final Object value : map.values()) {
                final Object found = relation(value);
                if (found != null) {
                    return found;
                }
            }
        } else if (node instanceof Iterable) {
            for (final Object value : (Iterable<Object>) node) {
                final Object found = relation(value);
                if (found != null) {
                    return found;
                }
            }
        }
        return null;
    }
}
