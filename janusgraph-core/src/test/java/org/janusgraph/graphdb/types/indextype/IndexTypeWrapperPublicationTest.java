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

package org.janusgraph.graphdb.types.indextype;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Modifier;

import static org.junit.jupiter.api.Assertions.assertTrue;

//These fields cache a value which is built on first read rather than in the constructor. Nothing orders the writes
//which populate that value before the write which publishes the reference to it, so without volatile another thread
//can observe the reference while the value it points at is still half built.
//
//RelationTypeVertex.getKeyIndexes caches the IndexType instances of a property key, so every caller on that vertex is
//handed the same wrapper. Several threads reach one wrapper when they share a transaction, which a transaction that is
//not thread bound supports: the vertexCache of StandardJanusGraphTx is concurrent for exactly that reason.
//
//The race cannot be reproduced reliably in a test. It needs a reordering which a given JVM and processor may never
//perform, so a thread based test would pass whether or not the field is volatile. This asserts the property itself
//instead. A field which is final is accepted too, so that moving the cache behind an immutable holder does not fail
//here.
public class IndexTypeWrapperPublicationTest {

    @Test
    public void mixedIndexFieldsShouldBePublishedSafely() throws Exception {
        assertSafelyPublished(MixedIndexTypeWrapper.class, "fields");
    }

    @Test
    public void compositeIndexCachesShouldBePublishedSafely() throws Exception {
        assertSafelyPublished(CompositeIndexTypeWrapper.class, "fields");
        assertSafelyPublished(CompositeIndexTypeWrapper.class, "inlineKeys");
    }

    @Test
    public void sharedIndexTypeCachesShouldBePublishedSafely() throws Exception {
        assertSafelyPublished(IndexTypeWrapper.class, "fieldMap");
        assertSafelyPublished(IndexTypeWrapper.class, "cachedTypeConstraint");
        assertSafelyPublished(IndexTypeWrapper.class, "schemaTypeConstraint");
    }

    private static void assertSafelyPublished(Class<?> declaringClass, String fieldName) throws NoSuchFieldException {
        final int modifiers = declaringClass.getDeclaredField(fieldName).getModifiers();
        assertTrue(Modifier.isVolatile(modifiers) || Modifier.isFinal(modifiers),
            declaringClass.getSimpleName() + "." + fieldName + " can be read by one thread while another writes it, "
                + "so it must be volatile or final to be published safely");
    }
}
