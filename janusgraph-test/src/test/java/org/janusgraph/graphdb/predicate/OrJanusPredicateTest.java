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
import org.janusgraph.core.attribute.Text;
import org.janusgraph.graphdb.query.JanusGraphPredicate;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author David Clement (david.clement90@laposte.net)
 */
public class OrJanusPredicateTest extends ConnectiveJanusPredicateTest{

    @Override
    ConnectiveJanusPredicate getPredicate(List<JanusGraphPredicate> childPredicates) {
        return new OrJanusPredicate(childPredicates);
    }

    @Override
    ConnectiveJanusPredicate getNegatePredicate(List<JanusGraphPredicate> childPredicates) {
        return new AndJanusPredicate(childPredicates);
    }

    @Test
    public void testIsQNF() {
        Assert.assertTrue(getPredicate(Arrays.asList(Text.PREFIX, Cmp.EQUAL)).isQNF());
        Assert.assertTrue(getPredicate(Arrays.asList(Text.PREFIX, Cmp.EQUAL, new OrJanusPredicate(Arrays.asList(Text.PREFIX, Cmp.EQUAL)))).isQNF());
        Assert.assertFalse(getPredicate(Arrays.asList(Text.PREFIX, Cmp.EQUAL, new AndJanusPredicate(Arrays.asList(Text.PREFIX, Cmp.EQUAL)))).isQNF());
    }
}
