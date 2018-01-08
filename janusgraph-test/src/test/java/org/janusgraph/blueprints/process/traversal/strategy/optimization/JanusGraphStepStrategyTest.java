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

package org.janusgraph.blueprints.process.traversal.strategy.optimization;

import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ElementValueComparator;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.FilterRankingStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.InlineFilterStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversalStrategies;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;

import org.janusgraph.StorageSetup;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.tinkerpop.optimize.HasStepFolder;
import org.janusgraph.graphdb.tinkerpop.optimize.JanusGraphStep;
import org.janusgraph.graphdb.tinkerpop.optimize.JanusGraphStepStrategy;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import static org.apache.tinkerpop.gremlin.process.traversal.P.eq;
import static org.apache.tinkerpop.gremlin.process.traversal.P.gt;
import static org.apache.tinkerpop.gremlin.process.traversal.P.lt;
import static org.apache.tinkerpop.gremlin.process.traversal.P.within;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.filter;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.properties;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.has;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.not;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.values;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class JanusGraphStepStrategyTest {

    @Parameterized.Parameter(value = 0)
    public Traversal original;

    @Parameterized.Parameter(value = 1)
    public Traversal optimized;

    @Parameterized.Parameter(value = 2)
    public Collection<TraversalStrategy> otherStrategies;

    @Test
    public void doTest() {
        final TraversalStrategies strategies = new DefaultTraversalStrategies();
        strategies.addStrategies(JanusGraphStepStrategy.instance());
        for (final TraversalStrategy strategy : this.otherStrategies) {
            strategies.addStrategies(strategy);
        }
        this.original.asAdmin().setStrategies(strategies);
        this.original.asAdmin().applyStrategies();
        assertEquals(this.optimized, this.original);
    }

    private static GraphTraversal.Admin<?, ?> g_V(final Object... hasKeyValues) {
        final GraphTraversal.Admin<?, ?> traversal = new DefaultGraphTraversal<>();
        final JanusGraphStep<Vertex, Vertex> graphStep = new JanusGraphStep<>(new GraphStep<>(traversal, Vertex.class, true));
        for (int i = 0; i < hasKeyValues.length; i = i + 2) {
            if (hasKeyValues[i].equals(T.id)) {
                graphStep.addIds(Arrays.asList(hasKeyValues[i + 1]));
            } else if (hasKeyValues[i] instanceof HasStepFolder.OrderEntry) {
                final HasStepFolder.OrderEntry orderEntry = (HasStepFolder.OrderEntry) hasKeyValues[i];
                graphStep.orderBy(orderEntry.key, orderEntry.order);
            } else {
                graphStep.addHasContainer(new HasContainer((String) hasKeyValues[i], (P) hasKeyValues[i + 1]));
            }
        }
        return traversal.addStep(graphStep);
    }

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<Object[]> generateTestParameters() {
        final StandardJanusGraph graph = (StandardJanusGraph) StorageSetup.getInMemoryGraph();
        final GraphTraversalSource g = graph.traversal();
        // create a basic schema so that order pushdown can be tested as this optimization requires a JanusGraph
        // transaction registered against a non-EmptyGraph
        final JanusGraphManagement mgmt = graph.openManagement();
        mgmt.makePropertyKey("name").dataType(String.class).make();
        mgmt.makePropertyKey("lang").dataType(String.class).make();
        mgmt.commit();

        return Arrays.asList(new Object[][]{
            {g.V().out(), g_V().out(), Collections.emptyList()},
            {g.V().has("name", "marko").out(), g_V("name", eq("marko")).out(), Collections.emptyList()},
            {g.V().has("name", "marko").has("age", gt(31).and(lt(10))).out(),
                    g_V("name", eq("marko"), "age", gt(31), "age", lt(10)).out(), Collections.emptyList()},
            {g.V().has("name", "marko").or(has("age"), has("age", gt(32))).has("lang", "java"),
                    g_V("name", eq("marko"), "lang", eq("java")).or(has("age"), has("age", gt(32))), Collections.singletonList(FilterRankingStrategy.instance())},
            {g.V().has("name", "marko").as("a").or(has("age"), has("age", gt(32))).has("lang", "java"),
                    g_V("name", eq("marko")).as("a").or(has("age"), has("age", gt(32))).has("lang", "java"), Collections.emptyList()},
            {g.V().has("name", "marko").as("a").or(has("age"), has("age", gt(32))).has("lang", "java"),
                    g_V("name", eq("marko"), "lang", eq("java")).or(has("age"), has("age", gt(32))).as("a"), Collections.singletonList(FilterRankingStrategy.instance())},
            {g.V().dedup().has("name", "marko").or(has("age"), has("age", gt(32))).has("lang", "java"),
                    g_V("name", eq("marko"), "lang", eq("java")).or(has("age"), has("age", gt(32))).dedup(), Collections.singletonList(FilterRankingStrategy.instance())},
            {g.V().as("a").dedup().has("name", "marko").or(has("age"), has("age", gt(32))).has("lang", "java"),
                    g_V("name", eq("marko"), "lang", eq("java")).or(has("age"), has("age", gt(32))).dedup().as("a"), Collections.singletonList(FilterRankingStrategy.instance())},
            {g.V().as("a").has("name", "marko").as("b").or(has("age"), has("age", gt(32))).has("lang", "java"),
                    g_V("name", eq("marko"), "lang", eq("java")).or(has("age"), has("age", gt(32))).as("b", "a"), Collections.singletonList(FilterRankingStrategy.instance())},
            {g.V().as("a").dedup().has("name", "marko").or(has("age"), has("age", gt(32))).filter(has("name", "bob")).has("lang", "java"),
                    g_V("name", eq("marko"), "lang", eq("java"), "name", eq("bob")).or(has("age"), has("age", gt(32))).dedup().as("a"), Arrays.asList(InlineFilterStrategy.instance(), FilterRankingStrategy.instance())},
            {g.V().has("name", "marko").or(not(has("age")), has("age", gt(32))).has("name", "bob").has("lang", "java"),
                    g_V("name", eq("marko"), "name", eq("bob"), "lang", eq("java")).or(not(filter(properties("age"))), has("age", gt(32))), TraversalStrategies.GlobalCache.getStrategies(JanusGraph.class).toList()},
            {g.V().has("name", eq("marko").and(eq("bob").and(eq("stephen")))).out("knows"),
                    g_V("name", eq("marko"), "name", eq("bob"), "name", eq("stephen")).out("knows"), Collections.emptyList()},
            {g.V().hasId(1), g_V(T.id, 1), Collections.emptyList()},
            {g.V().hasId(within(1, 2)), g_V(T.id, 1, T.id, 2), Collections.emptyList()},
            {g.V().hasId(1).has("name", "marko"), g_V(T.id, 1, "name", eq("marko")), Collections.emptyList()},
            {g.V().hasId(1).hasLabel("Person"), g_V(T.id, 1, "~label", eq("Person")), Collections.emptyList()},
            {g.V().hasLabel("Person").has("lang", "java").order().by("name"),
                g_V("~label", eq("Person"), "lang", eq("java"), new HasStepFolder.OrderEntry("name", Order.incr)), Collections.emptyList()},
            {g.V().hasLabel("Person").has("lang", "java").order().by(new ElementValueComparator("name", Order.incr)),
                g_V("~label", eq("Person"), "lang", eq("java"), new HasStepFolder.OrderEntry("name", Order.incr)), Collections.emptyList()},
            // same as above, different order
            {g.V().hasLabel("Person").has("lang", "java").order().by("name", Order.decr),
                g_V("~label", eq("Person"), "lang", eq("java"), new HasStepFolder.OrderEntry("name", Order.decr)), Collections.emptyList()},
            // if multiple order steps are specified in a row, only the last will be folded in because it overrides previous ordering
            {g.V().hasLabel("Person").has("lang", "java").order().by("lang", Order.incr).order().by("name", Order.decr),
                g_V("~label", eq("Person"), "lang", eq("java"), new HasStepFolder.OrderEntry("name", Order.decr)), Collections.emptyList()},
            // do not folder in orders that include a nested traversal
            {g.V().hasLabel("Person").order().by(values("age")),
                g_V("~label", eq("Person")).order().by(values("age")), Collections.emptyList()},
            // age property is not registered in the schema so the order should not be folded in
            {g.V().hasLabel("Person").has("lang", "java").order().by("age"),
                g_V("~label", eq("Person"), "lang", eq("java")).order().by("age"), Collections.emptyList()},
            // Per the TinkerGraph reference implementation, multiple hasIds in a row should not be folded
            // into a single within(ids) lookup
            {g.V().hasId(1).hasId(2), g_V(T.id, 1).hasId(2), Collections.emptyList()},
        });
    }
}
