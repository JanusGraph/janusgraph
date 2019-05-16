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

import java.util.Arrays;
import java.util.List;

import org.janusgraph.core.attribute.Cmp;
import org.janusgraph.core.attribute.Geo;
import org.janusgraph.core.attribute.Geoshape;
import org.janusgraph.core.attribute.Text;
import org.janusgraph.graphdb.query.JanusGraphPredicate;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

/**
 * @author David Clement (david.clement90@laposte.net)
 */
public abstract class ConnectiveJanusPredicateTest {


    abstract ConnectiveJanusPredicate getPredicate(List<JanusGraphPredicate> childPredicates);

    abstract ConnectiveJanusPredicate getNegatePredicate(List<JanusGraphPredicate> childPredicates);

    @Test
    public void testIsValidConditionNotAList() {
        Assert.assertFalse(getPredicate(Arrays.asList()).isValidCondition(3));
    }

    @Test
    public void testIsValidConditionDifferentSize() {
        Assert.assertFalse(getPredicate(Arrays.asList()).isValidCondition(Arrays.asList(3)));
    }

    @Test
    public void testIsValidConditionOk() {
        Assert.assertTrue(getPredicate(Arrays.asList(Text.CONTAINS, Cmp.EQUAL, Geo.WITHIN)).isValidCondition(Arrays.asList("john", 3, Geoshape.point(2.0, 4.0))));
    }

    @Test
    public void testIsValidConditionKoFirst() {
        Assert.assertFalse(getPredicate(Arrays.asList(Text.CONTAINS, Cmp.EQUAL, Geo.WITHIN)).isValidCondition(Arrays.asList(1L, 3, Geoshape.point(2.0, 4.0))));
    }

    @Test
    public void testIsValidConditionKo() {
        Assert.assertFalse(getPredicate(Arrays.asList(Text.CONTAINS, Cmp.EQUAL, Geo.WITHIN)).isValidCondition(Arrays.asList("john", 3, 1L)));
    }

    @Test
    public void testIsValidTypeOk() {
        Assert.assertTrue(getPredicate(Arrays.asList(Text.CONTAINS, Cmp.EQUAL)).isValidValueType(String.class));
    }

    @Test
    public void testIsValidKo() {
        Assert.assertFalse(getPredicate(Arrays.asList(Text.CONTAINS, Cmp.EQUAL)).isValidValueType(Integer.class));
    }

    @Test
    public void testHasNegationOk() {
        Assert.assertTrue(getPredicate(Arrays.asList(Geo.INTERSECT, Cmp.EQUAL)).hasNegation());
    }

    @Test
    public void testHasNegationKo() {
        Assert.assertFalse(getPredicate(Arrays.asList(Text.CONTAINS, Cmp.EQUAL)).hasNegation());
    }

    @Test
    public void testNegate() {
        Assert.assertEquals(getNegatePredicate(Arrays.asList(Geo.DISJOINT, Cmp.NOT_EQUAL)), getPredicate(Arrays.asList(Geo.INTERSECT, Cmp.EQUAL)).negate());
    }

    @Test
    public void testTestNotAList() {
        Assert.assertFalse(getPredicate(Arrays.asList()).test("john","jo"));
    }

    @Test
    public void testTestDifferentSize() {
        Assert.assertFalse(getPredicate(Arrays.asList()).test("john",Arrays.asList("jo")));
    }


    @Test
    public void testTest() {
        final ConnectiveJanusPredicate predicate = getPredicate(Arrays.asList(Text.PREFIX, Cmp.EQUAL));
        Assert.assertTrue(predicate.test("john",Arrays.asList("jo", "john")));
        Assert.assertEquals(predicate.isOr(), predicate.test("john",Arrays.asList("jo", "mike")));
        Assert.assertEquals(predicate.isOr(), predicate.test("john",Arrays.asList("mi", "john")));
        Assert.assertFalse(predicate.test("john",Arrays.asList("mi", "mike")));
    }

}
