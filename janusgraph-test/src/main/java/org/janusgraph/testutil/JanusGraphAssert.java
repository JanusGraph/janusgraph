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

package org.janusgraph.testutil;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Element;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.lang.reflect.Array;
import java.util.Collection;
import java.util.Iterator;
import java.util.stream.Stream;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class JanusGraphAssert {

    public static void assertCount(int expected, Traversal traversal) {
        org.junit.Assert.assertEquals(expected, traversal.toList().size());
    }

    public static void assertCount(int expected, Collection collection) {
        org.junit.Assert.assertEquals(expected, collection.size());
    }

    public static void assertCount(int expected, Iterable iterable) {
        org.junit.Assert.assertEquals(expected, Iterables.size(iterable));
    }

    public static void assertCount(int expected, Iterator iterator) {
        org.junit.Assert.assertEquals(expected, Iterators.size(iterator));
    }

    public static void assertCount(long expected, Stream stream) {
        org.junit.Assert.assertEquals(expected, stream.count());
    }

    public static<V extends Element> void assertEmpty(Object object) {
        org.junit.Assert.assertTrue(isEmpty(object));
    }

    public static<V extends Element> void assertNotEmpty(Object object) {
        org.junit.Assert.assertFalse(isEmpty(object));
    }

    public static<E extends Element> void assertTraversal(GraphTraversal<?, E> req, E... expectedElements) {
        for (final E expectedElement : expectedElements) {
            assertEquals(expectedElement, req.next());
        }
        assertFalse(req.hasNext());
    }

    private static boolean isEmpty(Object obj) {
        Preconditions.checkArgument(obj != null);
        if (obj instanceof Traversal) return !((Traversal) obj).hasNext();
        else if (obj instanceof Collection) return ((Collection)obj).isEmpty();
        else if (obj instanceof Iterable) return Iterables.isEmpty((Iterable)obj);
        else if (obj instanceof Iterator) return !((Iterator)obj).hasNext();
        else if (obj instanceof Stream) return ((Stream) obj).count() == 0;
        else if (obj.getClass().isArray()) return Array.getLength(obj)==0;
        throw new IllegalArgumentException("Cannot determine size of: " + obj);
    }

}
