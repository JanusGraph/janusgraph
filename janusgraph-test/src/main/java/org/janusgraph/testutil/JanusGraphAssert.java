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
import org.apache.tinkerpop.gremlin.structure.Element;

import java.lang.reflect.Array;
import java.util.Collection;
import java.util.Iterator;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class JanusGraphAssert {

    public static void assertCount(long expected, Object object) {
        org.junit.Assert.assertEquals(expected, size(object));
    }

    public static<V extends Element> void assertEmpty(Object object) {
        org.junit.Assert.assertTrue(isEmpty(object));
    }

    public static<V extends Element> void assertNotEmpty(Object object) {
        org.junit.Assert.assertFalse(isEmpty(object));
    }

    public static int size(Object obj) {
        Preconditions.checkArgument(obj != null);
        if (obj instanceof Traversal) return size(((Traversal) obj).toList());
        else if (obj instanceof Collection) return ((Collection)obj).size();
        else if (obj instanceof Iterable) return Iterables.size((Iterable) obj);
        else if (obj instanceof Iterator) return Iterators.size((Iterator)obj);
        else if (obj.getClass().isArray()) return Array.getLength(obj);
        throw new IllegalArgumentException("Cannot determine size of: " + obj);
    }

    public static boolean isEmpty(Object obj) {
        Preconditions.checkArgument(obj != null);
        if (obj instanceof Traversal) return !((Traversal) obj).hasNext();
        else if (obj instanceof Collection) return ((Collection)obj).isEmpty();
        else if (obj instanceof Iterable) return Iterables.isEmpty((Iterable)obj);
        else if (obj instanceof Iterator) return !((Iterator)obj).hasNext();
        else if (obj.getClass().isArray()) return Array.getLength(obj)==0;
        throw new IllegalArgumentException("Cannot determine size of: " + obj);
    }

}
