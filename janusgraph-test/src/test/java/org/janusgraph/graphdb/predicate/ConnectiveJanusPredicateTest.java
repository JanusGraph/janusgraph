/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.janusgraph.graphdb.predicate;

import org.janusgraph.core.attribute.Cmp;
import org.janusgraph.core.attribute.Geo;
import org.janusgraph.core.attribute.Geoshape;
import org.janusgraph.core.attribute.Text;
import org.janusgraph.graphdb.query.JanusGraphPredicate;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author David Clement (david.clement90@laposte.net)
 */
public abstract class ConnectiveJanusPredicateTest {


    abstract ConnectiveJanusPredicate getPredicate(List<JanusGraphPredicate> childPredicates);

    abstract ConnectiveJanusPredicate getNegatePredicate(List<JanusGraphPredicate> childPredicates);

    @Test
    public void testIsValidConditionNotAList() {
        assertFalse(getPredicate(emptyList()).isValidCondition(3));
    }

    @Test
    public void testIsValidConditionDifferentSize() {
        assertFalse(getPredicate(emptyList()).isValidCondition(singletonList(3)));
    }

    @Test
    public void testIsValidConditionOk() {
        assertTrue(getPredicate(Arrays.asList(Text.CONTAINS, Cmp.EQUAL, Geo.WITHIN)).isValidCondition(Arrays.asList("john", 3, Geoshape.point(2.0, 4.0))));
    }

    @Test
    public void testIsValidConditionKoFirst() {
        assertFalse(getPredicate(Arrays.asList(Text.CONTAINS, Cmp.EQUAL, Geo.WITHIN)).isValidCondition(Arrays.asList(1L, 3, Geoshape.point(2.0, 4.0))));
    }

    @Test
    public void testIsValidConditionKo() {
        assertFalse(getPredicate(Arrays.asList(Text.CONTAINS, Cmp.EQUAL, Geo.WITHIN)).isValidCondition(Arrays.asList("john", 3, 1L)));
    }

    @Test
    public void testIsValidTypeOk() {
        assertTrue(getPredicate(Arrays.asList(Text.CONTAINS, Cmp.EQUAL)).isValidValueType(String.class));
    }

    @Test
    public void testIsValidKo() {
        assertFalse(getPredicate(Arrays.asList(Text.CONTAINS, Cmp.EQUAL)).isValidValueType(Integer.class));
    }

    @Test
    public void testHasNegationOk() {
        assertTrue(getPredicate(Arrays.asList(Geo.INTERSECT, Cmp.EQUAL)).hasNegation());
    }

    @Test
    public void testNegate() {
        assertEquals(getNegatePredicate(Arrays.asList(Geo.DISJOINT, Cmp.NOT_EQUAL)), getPredicate(Arrays.asList(Geo.INTERSECT, Cmp.EQUAL)).negate());
    }

    @Test
    public void testTestNotAList() {
        assertFalse(getPredicate(emptyList()).test("john","jo"));
    }

    @Test
    public void testTestDifferentSize() {
        assertFalse(getPredicate(emptyList()).test("john", singletonList("jo")));
    }


    @Test
    public void testTest() {
        final ConnectiveJanusPredicate predicate = getPredicate(Arrays.asList(Text.PREFIX, Cmp.EQUAL));
        assertTrue(predicate.test("john",Arrays.asList("jo", "john")));
        assertEquals(predicate.isOr(), predicate.test("john",Arrays.asList("jo", "mike")));
        assertEquals(predicate.isOr(), predicate.test("john",Arrays.asList("mi", "john")));
        assertFalse(predicate.test("john",Arrays.asList("mi", "mike")));
    }
}
