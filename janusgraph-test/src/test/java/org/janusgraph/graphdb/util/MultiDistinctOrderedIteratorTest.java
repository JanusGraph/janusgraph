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

package org.janusgraph.graphdb.util;

import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.inmemory.InMemoryStoreManager;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.internal.Order;
import org.janusgraph.graphdb.internal.OrderList;
import org.janusgraph.graphdb.query.Query;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class MultiDistinctOrderedIteratorTest {

    private JanusGraph graph;
    private JanusGraphTransaction tx;

    @BeforeEach
    public void setUp() {
        final ModifiableConfiguration config = GraphDatabaseConfiguration.buildGraphConfiguration();
        config.set(GraphDatabaseConfiguration.STORAGE_BACKEND, InMemoryStoreManager.class.getCanonicalName());
        graph = JanusGraphFactory.open(config);
        tx = graph.newTransaction();
    }

    @AfterEach
    public void tearDown() {
        tx.rollback();
        graph.close();
    }

    @Test
    public void shouldMergeInTheOrderOfTheQueries() {
        final PropertyKey name = tx.makePropertyKey("name").dataType(String.class).make();
        for (final Order order : Order.values()) {
            final OrderList orders = new OrderList();
            orders.add(name, order);
            orders.makeImmutable();
            final List<Vertex> first = sorted(orders, "a", "\uE000", "\uD83D\uDE00", null);
            final List<Vertex> second = sorted(orders, "z", "\uFFFF", "\uD83D\uDE00a");
            final List<Vertex> all = new ArrayList<>(first);
            all.addAll(second);
            all.sort((Comparator) orders);
            assertEquals(all, merge((Comparator) orders, 0, Query.NO_LIMIT, first, second), order.toString());
        }
        //By code point, and an element without the key last in either direction, as OrderList compares them
        final OrderList ascending = new OrderList();
        ascending.add(name, Order.ASC);
        ascending.makeImmutable();
        assertEquals(Arrays.asList("a", "z", "\uE000", "\uFFFF", "\uD83D\uDE00", "\uD83D\uDE00a", null),
            names(merge((Comparator) ascending, 0, Query.NO_LIMIT,
                sorted(ascending, "a", "\uE000", "\uD83D\uDE00", null),
                sorted(ascending, "z", "\uFFFF", "\uD83D\uDE00a"))));
    }

    @Test
    public void shouldMergeEachElementOnceBetweenTheLimits() {
        final Comparator<Vertex> byId = Comparator.comparing(v -> (Integer) v.id());
        final List<Vertex> first = detached(0, 2, 4), second = detached(1, 2, 3, 5);
        assertEquals(detached(0, 1, 2, 3, 4, 5), merge(byId, 0, Query.NO_LIMIT, first, second));
        assertEquals(detached(1, 2, 3), merge(byId, 1, 4, first, second));
    }

    //Vertices of the given names, null for none, in the given order
    private List<Vertex> sorted(OrderList orders, String... names) {
        final List<Vertex> vertices = new ArrayList<>();
        for (final String name : names) {
            final JanusGraphVertex vertex = tx.addVertex();
            if (name != null) vertex.property("name", name);
            vertices.add(vertex);
        }
        vertices.sort((Comparator) orders);
        return vertices;
    }

    private static List<Vertex> detached(int... ids) {
        final List<Vertex> vertices = new ArrayList<>();
        for (final int id : ids) vertices.add(DetachedVertex.build().setId(id).create());
        return vertices;
    }

    @SafeVarargs
    private static List<Vertex> merge(Comparator<Vertex> order, int lowLimit, int highLimit, List<Vertex>... lists) {
        final List<Iterator<Vertex>> iterators = new ArrayList<>();
        for (final List<Vertex> list : lists) iterators.add(list.iterator());
        final MultiDistinctOrderedIterator<Vertex> merged =
            new MultiDistinctOrderedIterator<>(lowLimit, highLimit, iterators, order);
        final List<Vertex> result = new ArrayList<>();
        merged.forEachRemaining(result::add);
        merged.close();
        return result;
    }

    private static List<String> names(List<Vertex> vertices) {
        final List<String> names = new ArrayList<>();
        for (final Vertex vertex : vertices) {
            final Property<String> name = vertex.property("name");
            names.add(name.isPresent() ? name.value() : null);
        }
        return names;
    }
}
