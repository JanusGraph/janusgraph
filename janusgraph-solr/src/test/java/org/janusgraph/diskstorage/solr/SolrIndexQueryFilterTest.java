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

package org.janusgraph.diskstorage.solr;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.JanusGraphElement;
import org.janusgraph.core.attribute.Cmp;
import org.janusgraph.core.attribute.Geo;
import org.janusgraph.core.attribute.Geoshape;
import org.janusgraph.core.attribute.Text;
import org.janusgraph.core.schema.Mapping;
import org.janusgraph.core.schema.Parameter;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.indexing.KeyInformation;
import org.janusgraph.diskstorage.indexing.StandardKeyInformation;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.query.condition.And;
import org.janusgraph.graphdb.query.condition.Condition;
import org.janusgraph.graphdb.query.condition.Not;
import org.janusgraph.graphdb.query.condition.Or;
import org.janusgraph.graphdb.query.condition.PredicateCondition;
import org.janusgraph.graphdb.types.ParameterType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * The filter queries which Solr gets for conditions. Solr's standard query parser rejects a "+" before a clause which
 * starts with one, applies the "+" and "-" clauses of an alternative to the whole disjunction, and matches nothing
 * with a group of only "-" clauses. So a condition nested in another one has to be written as a query which means on
 * its own what it means there. The queries which were right before are unchanged.
 */
public class SolrIndexQueryFilterTest {

    private static final String INDEX = "solr";
    private static final String NUM = "num_i";
    private static final String TEXT = "text_t";
    private static final String NAME = "name_s";
    private static final String POINT = "point_g";
    private static final String SHAPE = "shape_g";
    private static final Map<String, KeyInformation> KEYS = new HashMap<>();

    static {
        KEYS.put(NUM, new StandardKeyInformation(Integer.class, Cardinality.SINGLE));
        KEYS.put(TEXT, new StandardKeyInformation(String.class, Cardinality.SINGLE, Mapping.TEXT.asParameter()));
        // an analyzer which splits a string into its words
        KEYS.put(NAME, new StandardKeyInformation(String.class, Cardinality.SINGLE, Mapping.STRING.asParameter(),
            new Parameter<>(ParameterType.STRING_ANALYZER.getName(), StandardAnalyzer.class.getName())));
        KEYS.put(POINT, new StandardKeyInformation(Geoshape.class, Cardinality.SINGLE));
        KEYS.put(SHAPE, new StandardKeyInformation(Geoshape.class, Cardinality.SINGLE,
            Mapping.PREFIX_TREE.asParameter()));
    }

    private static SolrIndex index;

    @BeforeAll
    public static void openIndex() throws BackendException {
        // The HTTP mode doesn't connect to Solr before the first request
        final ModifiableConfiguration config = GraphDatabaseConfiguration.buildGraphConfiguration();
        config.set(SolrIndex.SOLR_MODE, "http", INDEX);
        index = new SolrIndex(config.restrictTo(INDEX));
    }

    @AfterAll
    public static void closeIndex() throws BackendException {
        index.close();
    }

    private static String filter(Condition<JanusGraphElement> condition) {
        return index.buildQueryFilter(condition, KEYS::get);
    }

    private static Condition<JanusGraphElement> num(Cmp cmp, int value) {
        return PredicateCondition.of(NUM, cmp, value);
    }

    private static Condition<JanusGraphElement> words(String words) {
        return PredicateCondition.of(TEXT, Text.CONTAINS, words);
    }

    @SafeVarargs
    private static Condition<JanusGraphElement> and(Condition<JanusGraphElement>... children) {
        return And.of(children);
    }

    @SafeVarargs
    private static Condition<JanusGraphElement> or(Condition<JanusGraphElement>... children) {
        return Or.of(children);
    }

    private static Condition<JanusGraphElement> not(Condition<JanusGraphElement> child) {
        return Not.of(child);
    }

    @Test
    public void testQueriesWhichWereRightAreUnchanged() {
        assertEquals("num_i:1", filter(num(Cmp.EQUAL, 1)));
        assertEquals("num_i:* -num_i:1", filter(num(Cmp.NOT_EQUAL, 1)));
        assertEquals("text_t:(red)", filter(words("red")));
        // the analyzer splits the words, which are a conjunction
        assertEquals("+text_t:(red) +text_t:(green) ", filter(words("red green")));
        assertEquals("num_i:1 ", filter(and(num(Cmp.EQUAL, 1))));
        assertEquals("+num_i:1 +num_i:[* TO 5} ", filter(and(num(Cmp.EQUAL, 1), num(Cmp.LESS_THAN, 5))));
        assertEquals("+num_i:1 +num_i:* -num_i:2 ", filter(and(num(Cmp.EQUAL, 1), num(Cmp.NOT_EQUAL, 2))));
        assertEquals("+num_i:1 -(num_i:2) ", filter(and(num(Cmp.EQUAL, 1), not(num(Cmp.EQUAL, 2)))));
        assertEquals("-(num_i:1) -(num_i:2) ", filter(and(not(num(Cmp.EQUAL, 1)), not(num(Cmp.EQUAL, 2)))));
        assertEquals("+(num_i:1 OR num_i:2) +num_i:[* TO 5} ",
            filter(and(or(num(Cmp.EQUAL, 1), num(Cmp.EQUAL, 2)), num(Cmp.LESS_THAN, 5))));
        assertEquals("(num_i:1 OR num_i:{3 TO *])", filter(or(num(Cmp.EQUAL, 1), num(Cmp.GREATER_THAN, 3))));
        assertEquals("-(num_i:1)", filter(not(num(Cmp.EQUAL, 1))));
        assertEquals("-(num_i:* -num_i:1)", filter(not(num(Cmp.NOT_EQUAL, 1))));
        assertEquals("-(+num_i:1 +num_i:[* TO 5} )", filter(not(and(num(Cmp.EQUAL, 1), num(Cmp.LESS_THAN, 5)))));
        assertEquals("-((num_i:1 OR num_i:2))", filter(not(or(num(Cmp.EQUAL, 1), num(Cmp.EQUAL, 2)))));
    }

    @Test
    public void testConjunctionInAConjunctionIsRequiredAsAGroup() {
        // "++num_i:1 +num_i:[* TO 5} +num_i:{0 TO *] " doesn't parse
        assertEquals("+(+num_i:1 +num_i:[* TO 5} ) +num_i:{0 TO *] ",
            filter(and(and(num(Cmp.EQUAL, 1), num(Cmp.LESS_THAN, 5)), num(Cmp.GREATER_THAN, 0))));
        assertEquals("+(+text_t:(red) +text_t:(green) ) +num_i:1 ", filter(and(words("red green"), num(Cmp.EQUAL, 1))));
        assertEquals("+(+name_s:\"red\" +name_s:\"green\" ) +num_i:1 ",
            filter(and(PredicateCondition.of(NAME, Cmp.EQUAL, "red green"), num(Cmp.EQUAL, 1))));
        // one which starts with a negation joins the conjunction's clauses as before
        assertEquals("-(num_i:1) +num_i:[* TO 5}  +num_i:{0 TO *] ",
            filter(and(and(not(num(Cmp.EQUAL, 1)), num(Cmp.LESS_THAN, 5)), num(Cmp.GREATER_THAN, 0))));
    }

    @Test
    public void testAlternativeOfSeveralClausesIsAGroup() {
        // "(+num_i:1 +num_i:[* TO 5}  OR num_i:7)" would require both clauses of the first alternative
        assertEquals("((+num_i:1 +num_i:[* TO 5} ) OR num_i:7)",
            filter(or(and(num(Cmp.EQUAL, 1), num(Cmp.LESS_THAN, 5)), num(Cmp.EQUAL, 7))));
        assertEquals("((+text_t:(red) +text_t:(green) ) OR text_t:(yellow))",
            filter(or(words("red green"), words("yellow"))));
        // "(num_i:* -num_i:1 OR num_i:1)" would exclude 1 from all of it
        assertEquals("((num_i:* -num_i:1) OR num_i:1)", filter(or(num(Cmp.NOT_EQUAL, 1), num(Cmp.EQUAL, 1))));
        // a group of only exclusions starts from all documents
        assertEquals("(num_i:1 OR (*:* -(num_i:2)))", filter(or(num(Cmp.EQUAL, 1), not(num(Cmp.EQUAL, 2)))));
        assertEquals("((*:* -(num_i:1) -(num_i:2) ) OR num_i:1)",
            filter(or(and(not(num(Cmp.EQUAL, 1)), not(num(Cmp.EQUAL, 2))), num(Cmp.EQUAL, 1))));
        assertEquals("((num_i:1 ) OR num_i:2)", filter(or(and(num(Cmp.EQUAL, 1)), num(Cmp.EQUAL, 2))));
    }

    @Test
    public void testNegationOfOnlyExclusionsStartsFromAllDocuments() {
        // "-(-(num_i:1))" would exclude nothing
        assertEquals("-(*:* -(num_i:1))", filter(not(not(num(Cmp.EQUAL, 1)))));
        assertEquals("-(*:* -(num_i:1) -(num_i:2) )",
            filter(not(and(not(num(Cmp.EQUAL, 1)), not(num(Cmp.EQUAL, 2))))));
    }

    @Test
    public void testPrecisionOfAShapeIsAnOptionOfItsQuery() {
        final Geoshape polygon = Geoshape.polygon(Arrays.asList(new double[][]
            {{-1.0, 48.0}, {2.0, 48.0}, {2.0, 49.0}, {-1.0, 49.0}, {-1.0, 48.0}}));
        final Geoshape circle = Geoshape.circle(48.5, 0.5, 200.0);
        // within the quotes of the shape's query, where Solr reads its options, for each predicate a prefix tree takes
        assertEquals("shape_g:\"Intersects(" + polygon + ") distErrPct=0\"",
            filter(PredicateCondition.of(SHAPE, Geo.INTERSECT, polygon)));
        assertEquals("shape_g:\"IsWithin(" + circle + ") distErrPct=0\"",
            filter(PredicateCondition.of(SHAPE, Geo.WITHIN, circle)));
        assertEquals("shape_g:\"Contains(" + Geoshape.point(48.5, 0.5) + ") distErrPct=0\"",
            filter(PredicateCondition.of(SHAPE, Geo.CONTAINS, Geoshape.point(48.5, 0.5))));
        // geofilt takes no precision, and nothing follows it: a disjunction gets it as one alternative
        final String geofilt = "pt=48.5,0.5 d=" + circle.getRadius() + "}";
        assertEquals("{!geofilt sfield=point_g " + geofilt, filter(PredicateCondition.of(POINT, Geo.WITHIN, circle)));
        assertEquals("{!geofilt sfield=shape_g " + geofilt,
            filter(PredicateCondition.of(SHAPE, Geo.INTERSECT, circle)));
        assertEquals("({!geofilt sfield=point_g " + geofilt + " OR num_i:1)",
            filter(or(PredicateCondition.of(POINT, Geo.WITHIN, circle), num(Cmp.EQUAL, 1))));
        assertEquals("(shape_g:\"Intersects(" + polygon + ") distErrPct=0\" OR num_i:1)",
            filter(or(PredicateCondition.of(SHAPE, Geo.INTERSECT, polygon), num(Cmp.EQUAL, 1))));
    }
}
