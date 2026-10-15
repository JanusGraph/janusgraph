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

package org.janusgraph.graphdb.transaction.indexcache;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.core.JanusGraphVertexProperty;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.graphdb.internal.InternalRelation;
import org.janusgraph.graphdb.relations.StandardVertexProperty;
import org.janusgraph.graphdb.transaction.addedrelations.SimpleAddedRelations;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * The index of a transaction's added properties: built from the added relations on a key's first lookup, kept up to
 * date afterwards, by value, the properties of new vertices apart from the others; for both the cache of a transaction
 * bound to a thread and the one of a transaction which isn't.
 */
public class SimpleIndexCacheTest {

    private final PropertyKey uid = mock(PropertyKey.class);
    private final PropertyKey kind = mock(PropertyKey.class);
    private final JanusGraphVertex newVertex = vertex(true);
    private final JanusGraphVertex loadedVertex = vertex(false);

    public static Stream<Arguments> caches() {
        return Stream.of(
            Arguments.of("simple", (Function<SimpleAddedRelations, IndexCache>) SimpleIndexCache::new),
            Arguments.of("concurrent", (Function<SimpleAddedRelations, IndexCache>) ConcurrentIndexCache::new));
    }

    private static JanusGraphVertex vertex(boolean isNew) {
        final JanusGraphVertex vertex = mock(JanusGraphVertex.class);
        when(vertex.isNew()).thenReturn(isNew);
        return vertex;
    }

    private static StandardVertexProperty property(PropertyKey key, Object value, JanusGraphVertex vertex) {
        final StandardVertexProperty property = mock(StandardVertexProperty.class);
        when(property.isProperty()).thenReturn(true);
        when(property.getType()).thenReturn(key);
        when(property.propertyKey()).thenReturn(key);
        when(property.value()).thenReturn(value);
        when(property.element()).thenReturn(vertex);
        return property;
    }

    private static <T> ImmutableSet<T> set(Iterable<T> elements) {
        return ImmutableSet.copyOf(elements);
    }

    //What a lookup returns is the properties as they were at the lookup, which later additions and removals of the
    //same key and value don't change, so that a query can iterate them while the transaction changes
    @ParameterizedTest(name = "{0}")
    @MethodSource("caches")
    public void returnsThePropertiesAsTheyWereAtTheLookup(String name,
                                                           Function<SimpleAddedRelations, IndexCache> cacheOf) {
        final SimpleAddedRelations added = new SimpleAddedRelations(false);
        final IndexCache cache = cacheOf.apply(added);
        final StandardVertexProperty first = property(kind, "car", newVertex);
        final StandardVertexProperty second = property(kind, "car", newVertex);
        final StandardVertexProperty third = property(kind, "car", newVertex);
        final StandardVertexProperty other = property(kind, "bus", newVertex);
        for (final StandardVertexProperty property : Arrays.asList(first, second, third, other)) {
            added.add(property);
            cache.add(property);
        }
        final Collection<JanusGraphVertexProperty> cars = cache.get(kind, "car", true);
        final Iterable<JanusGraphVertexProperty> all = cache.getAll(kind, true);
        final Iterator<JanusGraphVertexProperty> iterating = all.iterator();
        iterating.next();

        added.remove(second);
        cache.remove(second);
        final StandardVertexProperty fourth = property(kind, "car", newVertex);
        added.add(fourth);
        cache.add(fourth);
        final StandardVertexProperty van = property(kind, "van", newVertex);
        added.add(van);
        cache.add(van);

        assertEquals(ImmutableSet.of(first, second, third), set(cars));
        assertEquals(ImmutableSet.of(first, second, third, other), set(all));
        int rest = 0;
        while (iterating.hasNext()) {
            iterating.next();
            rest++;
        }
        assertEquals(3, rest);
        assertEquals(ImmutableSet.of(first, third, fourth), set(cache.get(kind, "car", true)));
    }

    //All properties of a key come from an array which a later property goes after, or into a larger copy of, and which
    //a removal drops for the next lookup to build again: none of that changes what was returned before, and what is
    //returned after a removal no longer holds the removed property
    @ParameterizedTest(name = "{0}")
    @MethodSource("caches")
    public void returnsAllPropertiesAsTheyWereThroughLaterAdditionsAndRemovals(
            String name, Function<SimpleAddedRelations, IndexCache> cacheOf) {
        final SimpleAddedRelations added = new SimpleAddedRelations(false);
        final IndexCache cache = cacheOf.apply(added);
        final List<StandardVertexProperty> properties = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            properties.add(property(uid, "u" + i, newVertex));
            added.add(properties.get(i));
            cache.add(properties.get(i));
        }
        final Iterable<JanusGraphVertexProperty> ofThree = cache.getAll(uid, true);
        final Iterator<JanusGraphVertexProperty> iterating = ofThree.iterator();
        iterating.next();

        //Enough more for the array to grow, and one indexed again, which changes nothing
        for (int i = 3; i < 40; i++) {
            properties.add(property(uid, "u" + i, newVertex));
            added.add(properties.get(i));
            cache.add(properties.get(i));
        }
        cache.add(properties.get(0));
        final Iterable<JanusGraphVertexProperty> ofForty = cache.getAll(uid, true);
        assertEquals(ImmutableSet.copyOf(properties.subList(0, 3)), set(ofThree));
        assertEquals(3, Iterables.size(ofThree));
        assertEquals(ImmutableSet.copyOf(properties), set(ofForty));
        assertEquals(40, Iterables.size(ofForty));

        //A removal, then another property of a value which a property has already
        final StandardVertexProperty removed = properties.remove(1);
        added.remove(removed);
        cache.remove(removed);
        final StandardVertexProperty twin = property(uid, "u0", newVertex);
        properties.add(twin);
        added.add(twin);
        cache.add(twin);
        final Iterable<JanusGraphVertexProperty> afterTheRemoval = cache.getAll(uid, true);
        assertEquals(ImmutableSet.copyOf(properties), set(afterTheRemoval));
        assertEquals(40, Iterables.size(afterTheRemoval));
        assertEquals(3, Iterables.size(ofThree));
        assertEquals(40, Iterables.size(ofForty));
        assertTrue(set(ofForty).contains(removed));
        assertEquals(2, Iterators.size(iterating));

        //The next property goes after the others again
        final StandardVertexProperty last = property(uid, "last", newVertex);
        added.add(last);
        cache.add(last);
        assertEquals(40, Iterables.size(afterTheRemoval));
        assertEquals(41, Iterables.size(cache.getAll(uid, true)));
        assertTrue(set(cache.getAll(uid, true)).contains(last));
    }

    //A build which fails, here in reading the added relations, leaves the key to its next lookup, which builds the
    //whole index rather than finding a part of it
    @ParameterizedTest(name = "{0}")
    @MethodSource("caches")
    public void aBuildWhichFailsLeavesTheKeyToItsNextLookup(String name,
                                                            Function<SimpleAddedRelations, IndexCache> cacheOf) {
        final AtomicBoolean failing = new AtomicBoolean(true);
        final SimpleAddedRelations added = new SimpleAddedRelations(false) {
            @Override
            public Iterable<InternalRelation> getViewOfProperties(Predicate<InternalRelation> filter) {
                if (failing.getAndSet(false)) {
                    throw new IllegalStateException("can't read the added relations");
                }
                return super.getViewOfProperties(filter);
            }
        };
        final IndexCache cache = cacheOf.apply(added);
        final StandardVertexProperty property = property(uid, "a", newVertex);
        added.add(property);
        cache.add(property);

        assertThrows(IllegalStateException.class, () -> cache.get(uid, "a", true));
        assertEquals(ImmutableSet.of(property), set(cache.get(uid, "a", true)));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("caches")
    public void buildsAKeysIndexFromTheAddedRelationsAndKeepsItUpToDate(
            String name, Function<SimpleAddedRelations, IndexCache> cacheOf) {
        final SimpleAddedRelations added = new SimpleAddedRelations(false);
        final IndexCache cache = cacheOf.apply(added);
        final StandardVertexProperty before = property(uid, "a", newVertex);
        added.add(before);
        cache.add(before);
        added.add(property(kind, "x", newVertex));

        //The first lookup of the key indexes what was added before it
        assertEquals(1, cache.count(uid, "a", true));
        assertEquals(ImmutableSet.of(before), set(cache.get(uid, "a", true)));
        assertTrue(cache.get(uid, "a", false).isEmpty());

        //From then on, each property is indexed as it is added, apart by the vertex's being new
        final StandardVertexProperty ofLoaded = property(uid, "b", loadedVertex);
        added.add(ofLoaded);
        cache.add(ofLoaded);
        assertEquals(ImmutableSet.of(ofLoaded), set(cache.get(uid, "b", false)));
        assertEquals(0, cache.count(uid, "b", true));
        assertEquals(ImmutableSet.of(before), set(cache.getAll(uid, true)));
        assertEquals(ImmutableSet.of(ofLoaded), set(cache.getAll(uid, false)));

        //A key which isn't looked up isn't indexed until it is
        final StandardVertexProperty otherKind = property(kind, "y", loadedVertex);
        added.add(otherKind);
        cache.add(otherKind);
        assertEquals(ImmutableSet.of(otherKind), set(cache.get(kind, "y", false)));

        added.remove(ofLoaded);
        cache.remove(ofLoaded);
        assertTrue(cache.get(uid, "b", false).isEmpty());
        assertTrue(set(cache.getAll(uid, false)).isEmpty());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("caches")
    public void keepsSeveralPropertiesOfOneValueAndForgetsThemOneByOne(
            String name, Function<SimpleAddedRelations, IndexCache> cacheOf) {
        final SimpleAddedRelations added = new SimpleAddedRelations(false);
        final IndexCache cache = cacheOf.apply(added);
        assertEquals(0, cache.count(kind, "car", true));
        final StandardVertexProperty first = property(kind, "car", newVertex);
        final StandardVertexProperty second = property(kind, "car", vertex(true));
        final StandardVertexProperty third = property(kind, "car", vertex(true));
        for (StandardVertexProperty property : Arrays.asList(first, second, third)) {
            added.add(property);
            cache.add(property);
        }
        //Indexing a property again changes nothing
        cache.add(first);
        assertEquals(3, cache.count(kind, "car", true));
        assertEquals(ImmutableSet.of(first, second, third), set(cache.get(kind, "car", true)));

        cache.remove(second);
        assertEquals(ImmutableSet.of(first, third), set(cache.get(kind, "car", true)));
        //The property is forgotten whatever its vertex is by the time it is removed
        when(newVertex.isNew()).thenReturn(false);
        cache.remove(first);
        cache.remove(third);
        assertEquals(0, cache.count(kind, "car", true));
        assertTrue(set(cache.getAll(kind, true)).isEmpty());
    }
}
