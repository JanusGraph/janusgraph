package com.thinkaurelius.titan.graphdb;

import com.thinkaurelius.titan.core.Cardinality;
import com.thinkaurelius.titan.core.Multiplicity;
import com.thinkaurelius.titan.core.Order;
import com.tinkerpop.blueprints.Direction;
import org.junit.Test;

import static org.junit.Assert.*;

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
