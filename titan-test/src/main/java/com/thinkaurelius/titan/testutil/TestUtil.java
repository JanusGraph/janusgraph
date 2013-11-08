package com.thinkaurelius.titan.testutil;

import com.thinkaurelius.titan.core.Order;
import com.tinkerpop.blueprints.Element;

import java.util.Iterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class TestUtil {

    public static void verifyElementOrder(Iterable<? extends Element> elements, String key, Order order, int expectedCount) {
        verifyElementOrder(elements.iterator(), key, order, expectedCount);
    }

    public static void verifyElementOrder(Iterator<? extends Element> elements, String key, Order order, int expectedCount) {
        Comparable previous = null;
        int count = 0;
        while (elements.hasNext()) {
            Element element = elements.next();
            Comparable current = (Comparable)element.getProperty(key);
            if (previous != null) {
                int cmp = previous.compareTo(current);
                assertTrue(previous + " <> " + current + " @ " + count,
                        order == Order.ASC ? cmp <= 0 : cmp >= 0);
            }
            previous = current;
            count++;
        }
        assertEquals(expectedCount, count);
    }


}
