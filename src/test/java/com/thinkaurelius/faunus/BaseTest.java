package com.thinkaurelius.faunus;

import junit.framework.TestCase;

import java.util.LinkedList;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class BaseTest extends TestCase {

    public static <T> List<T> asList(final Iterable<T> iterable) {
        final List<T> list = new LinkedList<T>();
        for (final T t : iterable) {
            list.add(t);
        }
        return list;
    }


    public void testTrue() {
        assertTrue(true);
    }
}
