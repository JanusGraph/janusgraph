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

package org.janusgraph.graphdb;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.Multiplicity;
import org.janusgraph.graphdb.internal.Order;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests classes, enums and other non-interfaces in the core package
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class TestCoreElements {

    @Test
    public void testMultiplicityCardinality() {
        assertEquals(Multiplicity.MULTI,Multiplicity.convert(Cardinality.LIST));
        assertEquals(Multiplicity.SIMPLE,Multiplicity.convert(Cardinality.SET));
        assertEquals(Multiplicity.MANY2ONE,Multiplicity.convert(Cardinality.SINGLE));

        assertEquals(Multiplicity.MULTI.getCardinality(),Cardinality.LIST);
        assertEquals(Multiplicity.SIMPLE.getCardinality(),Cardinality.SET);
        assertEquals(Multiplicity.MANY2ONE.getCardinality(),Cardinality.SINGLE);

        assertFalse(Multiplicity.MULTI.isConstrained());
        assertTrue(Multiplicity.SIMPLE.isConstrained());
        assertTrue(Multiplicity.ONE2ONE.isConstrained());

        assertTrue(Multiplicity.ONE2ONE.isConstrained(Direction.BOTH));
        assertTrue(Multiplicity.SIMPLE.isConstrained(Direction.BOTH));

        assertFalse(Multiplicity.MULTI.isUnique(Direction.OUT));
        assertTrue(Multiplicity.MANY2ONE.isUnique(Direction.OUT));
    }

    @Test
    public void testOrder() {
        assertTrue(Order.ASC.modulateNaturalOrder("A".compareTo("B"))<0);
        assertTrue(Order.DESC.modulateNaturalOrder("A".compareTo("B"))>0);
    }


}
