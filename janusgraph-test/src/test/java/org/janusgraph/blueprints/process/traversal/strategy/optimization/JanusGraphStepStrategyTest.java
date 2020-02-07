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
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.OrStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ElementValueComparator;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.FilterRankingStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.InlineFilterStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.AndP;
import org.apache.tinkerpop.gremlin.process.traversal.util.ConnectiveP;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.util.OrP;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.janusgraph.StorageSetup;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.predicate.ConnectiveJanusPredicate;
import org.janusgraph.graphdb.query.JanusGraphPredicate.Converter;
import org.janusgraph.graphdb.tinkerpop.optimize.HasStepFolder;
import org.janusgraph.graphdb.tinkerpop.optimize.JanusGraphStep;
import org.janusgraph.graphdb.tinkerpop.optimize.JanusGraphStepStrategy;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.stream.Stream;

import static org.apache.tinkerpop.gremlin.process.traversal.P.eq;
import static org.apache.tinkerpop.gremlin.process.traversal.P.gt;
import static org.apache.tinkerpop.gremlin.process.traversal.P.lt;
import static org.apache.tinkerpop.gremlin.process.traversal.P.within;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.filter;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.properties;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.has;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.not;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.values;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;

public class JanusGraphStepStrategyTest {
    
    @ParameterizedTest
    @MethodSource("generateTestParameters")
    public void doTest(Traversal original, Traversal optimized, Collection<TraversalStrategy> otherStrategies) {
        final TraversalStrategies strategies = new DefaultTraversalStrategies();
        strategies.addStrategies(JanusGraphStepStrategy.instance());
        for (final TraversalStrategy strategy : otherStrategies) {
            strategies.addStrategies(strategy);
        }
        original.asAdmin().setStrategies(strategies);
        original.asAdmin().applyStrategies();
        assertEquals(optimized, original);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static GraphTraversal.Admin<?, ?> g_V(final Object... hasKeyValues) {
        final GraphTraversal.Admin<?, ?> traversal = new DefaultGraphTraversal<>();
        final JanusGraphStep<Vertex, Vertex> graphStep = new JanusGraphStep<>(new GraphStep<>(traversal, Vertex.class, true));
        for (int i = 0; i < hasKeyValues.length; i++) {
            if(hasKeyValues[i].equals(T.id)) {
                graphStep.addIds(Arrays.asList(hasKeyValues[i + 1]));
                i++;
            } else if (hasKeyValues[i] instanceof HasStepFolder.OrderEntry) {
                final HasStepFolder.OrderEntry orderEntry = (HasStepFolder.OrderEntry) hasKeyValues[i];
                graphStep.orderBy(orderEntry.key, orderEntry.order);
            } else if (hasKeyValues[i] instanceof DefaultGraphTraversal &&  ((DefaultGraphTraversal) hasKeyValues[i]).getStartStep() instanceof OrStep){
                for (final Traversal.Admin<?, ?> child : ((OrStep<?>) ((DefaultGraphTraversal) hasKeyValues[i]).getStartStep()).getLocalChildren()) {
                    final JanusGraphStep<Vertex, Vertex> localGraphStep = ((JanusGraphStep<Vertex, Vertex>) ((DefaultGraphTraversal) child).getStartStep());
                    graphStep.addLocalAll(localGraphStep.getHasContainers());
                    localGraphStep.getOrders().forEach(orderEntry -> graphStep.localOrderBy(localGraphStep.getHasContainers(), orderEntry.key, orderEntry.order));
                    graphStep.setLocalLimit(localGraphStep.getHasContainers(), localGraphStep.getLowLimit(), localGraphStep.getHighLimit());
                }
            } else if (hasKeyValues[i] instanceof DefaultGraphTraversal &&  ((DefaultGraphTraversal) hasKeyValues[i]).getStartStep() instanceof RangeGlobalStep){
                final RangeGlobalStep range = (RangeGlobalStep) ((DefaultGraphTraversal) hasKeyValues[i]).getStartStep();
                graphStep.setLimit((int) range.getLowRange(), (int) range.getHighRange());
            }  else if (i < hasKeyValues.length -1 && hasKeyValues[i + 1] instanceof ConnectiveP) {
                final ConnectiveJanusPredicate connectivePredicate = Converter.instanceConnectiveJanusPredicate((ConnectiveP) hasKeyValues[i + 1] );
                graphStep.addHasContainer(new HasContainer((String) hasKeyValues[i], new P<>(connectivePredicate, Converter.convert(((ConnectiveP<?>) hasKeyValues[i + 1]), connectivePredicate))));
                i++;
            } else {
                graphStep.addHasContainer(new HasContainer((String) hasKeyValues[i], (P) hasKeyValues[i + 1]));
                i++;
            }
        }
        return traversal.addStep(graphStep);
    }


    private static Stream<Arguments> generateTestParameters() {
        final StandardJanusGraph graph = (StandardJanusGraph) StorageSetup.getInMemoryGraph();
        final GraphTraversalSource g = graph.traversal();
        // create a basic schema so that order pushdown can be tested as this optimization requires a JanusGraph
        // transaction registered against a non-EmptyGraph
        final JanusGraphManagement mgmt = graph.openManagement();
        mgmt.makePropertyKey("name").dataType(String.class).make();
        mgmt.makePropertyKey("lang").dataType(String.class).make();
        mgmt.commit();


        return Arrays.stream(new Arguments[]{
            arguments(g.V().out(), g_V().out(), Collections.emptyList()),
            arguments(g.V().has("name", "marko").out(), g_V("name", eq("marko")).out(), Collections.emptyList()),
            arguments(g.V().has("name", "marko").has("age", gt(31).and(lt(10))).out(),
                g_V("name", eq("marko"), "age", new AndP<Integer>(Arrays.asList(gt(31), lt(10)))).out(), Collections.emptyList()),
            arguments(g.V().has("name", "marko").has("age", gt(31).or(lt(10))).out(),
                g_V("name", eq("marko"), "age", new OrP<Integer>(Arrays.asList(gt(31), lt(10)))).out(), Collections.emptyList()),
            arguments(g.V().has("name", "marko").has("age", gt(31).and(lt(10).or(gt(40)))).out(),
                g_V("name", eq("marko"), "age", new OrP<Integer>(Arrays.asList(gt(31), new AndP<Integer>(Arrays.asList(lt(10), gt(40)))))).out(), Collections.emptyList()),
            arguments(g.V().has("name", "marko").or(has("age"), has("age", gt(32))).has("lang", "java"),
                g_V("name", eq("marko"), "lang", eq("java")).or(has("age"), has("age", gt(32))), Collections.singletonList(FilterRankingStrategy.instance())),
            arguments(g.V().has("name", "marko").as("a").or(has("age"), has("age", gt(32))).has("lang", "java"),
                g_V("name", eq("marko")).as("a").or(has("age"), has("age", gt(32))).has("lang", "java"), Collections.emptyList()),
            arguments(g.V().has("name", "marko").as("a").or(has("age"), has("age", gt(32))).has("lang", "java"),
                g_V("name", eq("marko"), "lang", eq("java")).or(has("age"), has("age", gt(32))).as("a"), Collections.singletonList(FilterRankingStrategy.instance())),
            arguments(g.V().dedup().has("name", "marko").or(has("age"), has("age", gt(32))).has("lang", "java"),
                g_V("name", eq("marko"), "lang", eq("java")).or(has("age"), has("age", gt(32))).dedup(), Collections.singletonList(FilterRankingStrategy.instance())),
            arguments(g.V().as("a").dedup().has("name", "marko").or(has("age"), has("age", gt(32))).has("lang", "java"),
                g_V("name", eq("marko"), "lang", eq("java")).or(has("age"), has("age", gt(32))).dedup().as("a"), Collections.singletonList(FilterRankingStrategy.instance())),
            arguments(g.V().as("a").has("name", "marko").as("b").or(has("age"), has("age", gt(32))).has("lang", "java"),
                g_V("name", eq("marko"), "lang", eq("java")).or(has("age"), has("age", gt(32))).as("b", "a"), Collections.singletonList(FilterRankingStrategy.instance())),
            arguments(g.V().as("a").dedup().has("name", "marko").or(has("age"), has("age", gt(32))).filter(has("name", "bob")).has("lang", "java"),
                g_V("name", eq("marko"), "lang", eq("java"), "name", eq("bob")).or(has("age"), has("age", gt(32))).dedup().as("a"), Arrays.asList(InlineFilterStrategy.instance(), FilterRankingStrategy.instance())),
            arguments(g.V().has("name", "marko").or(not(has("age")), has("age", gt(32))).has("name", "bob").has("lang", "java"),
                g_V("name", eq("marko"), "name", eq("bob"), "lang", eq("java")).or(not(filter(properties("age"))), has("age", gt(32))), TraversalStrategies.GlobalCache.getStrategies(JanusGraph.class).toList()),
            arguments(g.V().has("name", eq("marko").and(eq("bob").and(eq("stephen")))).out("knows"),
                g_V("name", new AndP<String>(Arrays.asList(eq("marko"), eq("bob"), eq("stephen")))).out("knows"), Collections.emptyList()),
            arguments(g.V().hasId(1), g_V(T.id, 1), Collections.emptyList()),
            arguments(g.V().hasId(within(1, 2)), g_V(T.id, 1, T.id, 2), Collections.emptyList()),
            arguments(g.V().hasId(1).has("name", "marko"), g_V(T.id, 1, "name", eq("marko")), Collections.emptyList()),
            arguments(g.V().hasId(1).hasLabel("Person"), g_V(T.id, 1, "~label", eq("Person")), Collections.emptyList()),
            arguments(g.V().hasLabel("Person").has("lang", "java").order().by("name"),
                g_V("~label", eq("Person"), "lang", eq("java"), new HasStepFolder.OrderEntry("name", Order.incr)), Collections.emptyList()),
            arguments(g.V().hasLabel("Person").has("lang", "java").order().by(new ElementValueComparator("name", Order.incr)),
                g_V("~label", eq("Person"), "lang", eq("java"), new HasStepFolder.OrderEntry("name", Order.incr)), Collections.emptyList()),
            // same as above, different order
            arguments(g.V().hasLabel("Person").has("lang", "java").order().by("name", Order.decr),
                g_V("~label", eq("Person"), "lang", eq("java"), new HasStepFolder.OrderEntry("name", Order.decr)), Collections.emptyList()),
            arguments(g.V().hasLabel("Person").has("lang", "java").order().by(new ElementValueComparator("name", Order.incr)),
                g_V("~label", eq("Person"), "lang", eq("java"), new HasStepFolder.OrderEntry("name", Order.incr)), Collections.emptyList()),
            // if multiple order steps are specified in a row, only the last will be folded in because it overrides previous ordering
            arguments(g.V().hasLabel("Person").has("lang", "java").order().by("lang", Order.incr).order().by("name", Order.decr),
                g_V("~label", eq("Person"), "lang", eq("java"), new HasStepFolder.OrderEntry("name", Order.decr)), Collections.emptyList()),
            // do not folder in orders that include a nested traversal
            arguments(g.V().hasLabel("Person").order().by(values("age")),
                g_V("~label", eq("Person")).order().by(values("age")), Collections.emptyList()),
            // age property is not registered in the schema so the order should not be folded in
            arguments(g.V().hasLabel("Person").has("lang", "java").order().by("age"),
                g_V("~label", eq("Person"), "lang", eq("java")).order().by("age"), Collections.emptyList()),
            // Per the TinkerGraph reference implementation, multiple hasIds in a row should not be folded
            // into a single within(ids) lookup
            arguments(g.V().hasId(1).hasId(2), g_V(T.id, 1).hasId(2), Collections.emptyList()),
            arguments(g.V().has("name", "marko").range(10, 20), g_V("name", eq("marko"), __.range(10, 20)), Collections.emptyList()),
            arguments(g.V().has("name", "marko").or(has("length", lt(160)), has("age", gt(32))).has("lang", "java"),
                g_V("name", eq("marko"), "lang", eq("java"), __.or(g_V("length", lt(160)), g_V("age", gt(32)))), Collections.emptyList()),
            arguments(g.V().or(has("length", lt(160)), has("age", gt(32)).range(1, 5)),
                g_V(__.or(g_V("length", lt(160)), g_V("age", gt(32), __.range(1, 5)))), Collections.emptyList()),
            arguments(g.V().or(has("length", lt(160)), has("age", gt(32)).range(1, 5)).range(10, 20),
                g_V(__.or(g_V("length", lt(160)), g_V("age", gt(32), __.range(1, 5))), __.range(10, 20)), Collections.emptyList()),
            arguments(g.V().or(__.has("name", "marko"), has("lang", "java").order().by("name", Order.decr)),
                g_V(__.or(g_V("name", eq("marko")), g_V("lang", eq("java"), new HasStepFolder.OrderEntry("name", Order.decr)))), Collections.emptyList()),
            arguments(g.V().or(__.has("name", "marko"), has("lang", "java").order().by("name", Order.decr)).order().by("lang", Order.incr),
                g_V(__.or(g_V("name", eq("marko")), g_V("lang", eq("java"), new HasStepFolder.OrderEntry("name", Order.decr))), new HasStepFolder.OrderEntry("lang", Order.incr)), Collections.emptyList()),
            arguments(g.V().or(__.has("name", "marko").has("age", 29), __.has("name", "vadas").has("age", 27)).as("x").select("x"),
                g_V(__.or(g_V("name", eq("marko"), "age", eq(29)), g_V("name", eq("vadas"), "age", eq(27)))).as("x").select("x"), Collections.emptyList()),
        });
    }
}
