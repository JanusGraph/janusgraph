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

package org.janusgraph.graphdb.types.vertices;

import org.janusgraph.graphdb.types.VertexLabelVertex;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Modifier;

import static org.junit.jupiter.api.Assertions.assertTrue;

//These fields cache a mutable object which is built on first read and then published through the field. Nothing
//orders the writes which fill the object before the write of the reference, so without volatile another thread can
//observe the reference while the list or map behind it is still half built. A schema vertex is reached by every
//thread which shares a transaction, which a transaction that is not thread bound supports: the vertexCache of
//StandardJanusGraphTx is concurrent for exactly that reason. A half built indexes list hands IndexSerializer and the
//query planner fewer indexes than the key has, and nothing fails to say so.
//
//The race needs a reordering which a given JVM and processor may never perform, so a thread based test would pass
//whether or not the field is volatile. This asserts the property itself, as IndexTypeWrapperPublicationTest does for
//the wrappers these caches hand out. A final field is accepted too, so that moving a cache behind an immutable
//holder does not fail here. The caches which hold an immutable value - name, consistency, ttl and the
//ImmutableListMultimap relation caches - would publish safely without it, but a read which a reset overlapped must not
//keep what it loaded (SchemaVertexCacheResetTest), and that check only orders against the reset if every cache field is
//volatile.
public class SchemaVertexCachePublicationTest {

    @Test
    public void relationTypeCachesShouldBePublishedSafely() throws Exception {
        assertSafelyPublished(RelationTypeVertex.class, "indexes");
        assertSafelyPublished(RelationTypeVertex.class, "indexesReferences");
        assertSafelyPublished(RelationTypeVertex.class, "consistency");
        assertSafelyPublished(RelationTypeVertex.class, "ttl");
    }

    @Test
    public void vertexLabelCacheShouldBePublishedSafely() throws Exception {
        assertSafelyPublished(VertexLabelVertex.class, "ttl");
    }

    @Test
    public void schemaVertexCachesShouldBePublishedSafely() throws Exception {
        assertSafelyPublished(JanusGraphSchemaVertex.class, "definition");
        assertSafelyPublished(JanusGraphSchemaVertex.class, "name");
        assertSafelyPublished(JanusGraphSchemaVertex.class, "outRelations");
        assertSafelyPublished(JanusGraphSchemaVertex.class, "inRelations");
    }

    private static void assertSafelyPublished(Class<?> declaringClass, String fieldName) throws NoSuchFieldException {
        final int modifiers = declaringClass.getDeclaredField(fieldName).getModifiers();
        assertTrue(Modifier.isVolatile(modifiers) || Modifier.isFinal(modifiers),
            () -> declaringClass.getSimpleName() + "." + fieldName + " can be read by one thread while another writes "
                + "it, so it must be volatile or final to be published safely, but is declared ["
                + Modifier.toString(modifiers) + "]");
    }
}
