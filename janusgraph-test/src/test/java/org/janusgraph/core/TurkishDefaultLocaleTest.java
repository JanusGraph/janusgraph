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

package org.janusgraph.core;

import org.janusgraph.core.attribute.Geoshape;
import org.janusgraph.core.attribute.Text;
import org.janusgraph.core.schema.Mapping;
import org.janusgraph.core.schema.Parameter;
import org.janusgraph.diskstorage.indexing.StandardKeyInformation;
import org.janusgraph.diskstorage.util.time.Durations;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.spatial4j.context.jts.JtsSpatialContext;

import java.time.temporal.ChronoUnit;
import java.util.Locale;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Names which JanusGraph reads, and the text it lower-cases for case-insensitive search, are case converted in the root
 * locale, whatever the JVM's default locale is. In a Turkish locale I lower-cases to a dotless ı and i upper-cases to a
 * dotted İ, which neither the names nor the analyzers of the index backends know.
 */
@ResourceLock(Resources.LOCALE)
public class TurkishDefaultLocaleTest {

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
    public void aMappingIsFoundByItsNameInLowerCase() {
        assertEquals(Mapping.STRING, Mapping.getMapping(
            new StandardKeyInformation(String.class, Cardinality.SINGLE, Parameter.of("mapping", "string"))));
        assertEquals(Mapping.PREFIX_TREE, Mapping.getMapping(
            new StandardKeyInformation(Geoshape.class, Cardinality.SINGLE, Parameter.of("mapping", "prefix_tree"))));
    }

    @Test
    public void aShapeKnowsItsTypeByTheNameOfItsGeometry() {
        assertEquals(Geoshape.Type.MULTIPOINT, Geoshape.Type.fromGson("MultiPoint"));
        assertEquals(Geoshape.Type.GEOMETRYCOLLECTION, Geoshape.Type.fromGson("GeometryCollection"));
        final JtsSpatialContext context = (JtsSpatialContext) Geoshape.getSpatialContext();
        final Geoshape multiPoint = Geoshape.geoshape(context.getShapeFactory().makeShapeFromGeometry(
            new GeometryFactory().createMultiPoint(new Coordinate[] {new Coordinate(100, 0), new Coordinate(101, 1)})));
        assertEquals(Geoshape.Type.MULTIPOINT, multiPoint.getType());
    }

    @Test
    public void aTimeUnitIsFoundByItsNameInUpperCase() {
        assertEquals(ChronoUnit.MILLIS, Durations.parse("MILLIS"));
        assertEquals(ChronoUnit.MINUTES, Durations.parse("MINUTES"));
    }

    @Test
    public void aGraphOpensByTheShorthandOfItsBackendInUpperCase() {
        final JanusGraph graph = JanusGraphFactory.open("INMEMORY");
        try {
            assertTrue(graph.isOpen());
        } finally {
            graph.close();
        }
    }

    // The index backends' analyzers lower-case I to i, and so does the evaluation of a text predicate in memory
    @Test
    public void textPredicatesLowerCaseAsTheAnalyzersOfTheIndexBackendsDo() {
        assertTrue(Text.CONTAINS.test("ISTANBUL", "istanbul"));
        assertTrue(Text.CONTAINS_PREFIX.test("ISTANBUL", "ist"));
        assertTrue(Text.CONTAINS_PHRASE.test("VISIT ISTANBUL", "visit istanbul"));
        assertTrue(Text.CONTAINS_FUZZY.test("INFINITI", "infiniti"));
    }
}
