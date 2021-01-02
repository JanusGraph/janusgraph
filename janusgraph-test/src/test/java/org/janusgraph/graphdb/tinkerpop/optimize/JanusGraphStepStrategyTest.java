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

package org.janusgraph.graphdb.tinkerpop.optimize;

import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.IsStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.OrStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ElementValueComparator;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.FilterRankingStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.InlineFilterStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.AndP;
import org.apache.tinkerpop.gremlin.process.traversal.util.ConnectiveP;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.util.OrP;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.janusgraph.StorageSetup;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.predicate.ConnectiveJanusPredicate;
import org.janusgraph.graphdb.query.JanusGraphPredicateUtils;
import org.janusgraph.graphdb.tinkerpop.optimize.step.HasStepFolder;
import org.janusgraph.graphdb.tinkerpop.optimize.step.JanusGraphMultiQueryStep;
import org.janusgraph.graphdb.tinkerpop.optimize.step.JanusGraphStep;
import org.janusgraph.graphdb.tinkerpop.optimize.step.JanusGraphVertexStep;
import org.janusgraph.graphdb.tinkerpop.optimize.strategy.JanusGraphLocalQueryOptimizerStrategy;
import org.janusgraph.graphdb.tinkerpop.optimize.strategy.JanusGraphStepStrategy;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static org.apache.tinkerpop.gremlin.process.traversal.P.eq;
import static org.apache.tinkerpop.gremlin.process.traversal.P.gt;
import static org.apache.tinkerpop.gremlin.process.traversal.P.lt;
import static org.apache.tinkerpop.gremlin.process.traversal.P.neq;
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

    @ParameterizedTest
    @MethodSource("generateMultiQueryTestParameters")
    public void doMultiQueryTest(Traversal original, Traversal expected, Collection<TraversalStrategy> otherStrategies) {
        final TraversalStrategies strategies = new DefaultTraversalStrategies();
        strategies.addStrategies(JanusGraphStepStrategy.instance());
        for (final TraversalStrategy strategy : otherStrategies) {
            strategies.addStrategies(strategy);
        }

        // Can't add explicitly add JanusGraphVertexStep or JanusGraphMultiQueryStep to the expected traversal so add them now
        Traversal.Admin<?,?> optimized = expected.asAdmin();
        applyMultiQueryTraversalSteps(optimized);

        original.asAdmin().setStrategies(strategies);
        original.asAdmin().applyStrategies();

        // Tried using assertEquals(optimized, original), but if the traversal has any steps of type SideEffectStep that fails
        assertEquals(optimized.toString(), original.toString());
    }

    private void applyMultiQueryTraversalSteps(Traversal.Admin<?,?> traversal) {
        processVertexSteps(traversal);
        processChildTraversals(traversal);
        processIsMultiQuerySteps(traversal);
    }

    private void processVertexSteps(Traversal.Admin<?,?> traversal) {
        TraversalHelper.getStepsOfClass(VertexStep.class, traversal).forEach(vertexStep -> {
            JanusGraphVertexStep janusGraphVertexStep = new JanusGraphVertexStep<>(vertexStep);
            TraversalHelper.replaceStep(vertexStep, janusGraphVertexStep, traversal);
            if (JanusGraphTraversalUtil.isEdgeReturnStep(janusGraphVertexStep)) {
                HasStepFolder.foldInHasContainer(janusGraphVertexStep, traversal, traversal);
            }
        });
    }

    private void processChildTraversals(Traversal.Admin<?,?> traversal) {
        traversal.getSteps().forEach(step -> {
            if (JanusGraphTraversalUtil.isMultiQueryCompatibleStep((Step<?, ?>) step)) {
                ((TraversalParent)step).getGlobalChildren().forEach(child -> applyMultiQueryTraversalSteps(child));
                ((TraversalParent)step).getLocalChildren().forEach(child -> applyMultiQueryTraversalSteps(child));
            }
        });
    }

    /**
     * Replace any place holders like 'is(MQ_OPTIONAL)' with the expected JanusGraphMultiQueryStep
     * @param traversal The traversal to operate on
     */
    private void processIsMultiQuerySteps(Traversal.Admin<?,?> traversal) {
        TraversalHelper.getStepsOfClass(IsStep.class, traversal).forEach(isStep -> {
            Object expectedStep = isStep.getPredicate().getValue();
            Step<Vertex, ?> nextStep = isStep.getNextStep();
            if (expectedStep.equals(nextStep.getClass().getSimpleName())) {
                TraversalHelper.replaceStep(isStep, new JanusGraphMultiQueryStep(nextStep), traversal);
            }
        });
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static GraphTraversal.Admin<?, ?> g_V(final Object... hasKeyValues) {
        final GraphTraversal.Admin<?, ?> traversal = new DefaultGraphTraversal<>();
        final JanusGraphStep<Vertex, Vertex> graphStep = new JanusGraphStep<>(new GraphStep<>(traversal, Vertex.class, true));
        for (int i = 0; i < hasKeyValues.length; i++) {
            if(hasKeyValues[i].equals(T.id)) {
                graphStep.addIds(Collections.singletonList(hasKeyValues[i + 1]));
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
                final ConnectiveJanusPredicate connectivePredicate = JanusGraphPredicateUtils.instanceConnectiveJanusPredicate((ConnectiveP) hasKeyValues[i + 1] );
                graphStep.addHasContainer(new HasContainer((String) hasKeyValues[i], new P<>(connectivePredicate, JanusGraphPredicateUtils.convert(((ConnectiveP<?>) hasKeyValues[i + 1]), connectivePredicate))));
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
                g_V("name", eq("marko"), "lang", eq("java"), __.or(g_V("age", neq(null)), g_V("age", gt(32)))), Collections.singletonList(FilterRankingStrategy.instance())),
            arguments(g.V().has("name", "marko").as("a").or(has("age"), has("age", gt(32))).has("lang", "java"),
                g_V("name", eq("marko"), __.or(g_V("age", neq(null)), g_V("age", gt(32))), "lang", eq("java")).as("a"), Collections.emptyList()),
            arguments(g.V().has("name", "marko").as("a").or(has("age"), has("age", gt(32))).has("lang", "java"),
                g_V("name", eq("marko"), "lang", eq("java"), __.or(g_V("age", neq(null)), g_V("age", gt(32)))).as("a"), Collections.singletonList(FilterRankingStrategy.instance())),
            arguments(g.V().dedup().has("name", "marko").or(has("age"), has("age", gt(32))).has("lang", "java"),
                g_V("name", eq("marko"), "lang", eq("java"), __.or(g_V("age", neq(null)), g_V("age", gt(32)))).dedup(), Collections.singletonList(FilterRankingStrategy.instance())),
            arguments(g.V().as("a").dedup().has("name", "marko").or(has("age"), has("age", gt(32))).has("lang", "java"),
                g_V("name", eq("marko"), "lang", eq("java"), __.or(g_V("age", neq(null)), g_V("age", gt(32)))).dedup().as("a"), Collections.singletonList(FilterRankingStrategy.instance())),
            arguments(g.V().as("a").has("name", "marko").as("b").or(has("age"), has("age", gt(32))).has("lang", "java"),
                g_V("name", eq("marko"), "lang", eq("java"), __.or(g_V("age", neq(null)), g_V("age", gt(32)))).as("b", "a"), Collections.singletonList(FilterRankingStrategy.instance())),
            arguments(g.V().as("a").dedup().has("name", "marko").or(has("age"), has("age", gt(32))).filter(has("name", "bob")).has("lang", "java"),
                g_V("name", eq("marko"), "lang", eq("java"), "name", eq("bob"), __.or(g_V("age", neq(null)), g_V("age", gt(32)))).dedup().as("a"), Arrays.asList(InlineFilterStrategy.instance(), FilterRankingStrategy.instance())),
            arguments(g.V().has("name", "marko").or(not(has("age")), has("age", gt(32))).has("name", "bob").has("lang", "java"),
                g_V("name", eq("marko"), "name", eq("bob"), "lang", eq("java")).or(not(filter(properties("age"))), has("age", gt(32))), TraversalStrategies.GlobalCache.getStrategies(JanusGraph.class).toList()),
            arguments(g.V().has("name", eq("marko").and(eq("bob").and(eq("stephen")))).out("knows"),
                g_V("name", new AndP<String>(Arrays.asList(eq("marko"), eq("bob"), eq("stephen")))).out("knows"), Collections.emptyList()),
            arguments(g.V().hasId(1), g_V(T.id, 1), Collections.emptyList()),
            arguments(g.V().hasId(within(1, 2)), g_V(T.id, 1, T.id, 2), Collections.emptyList()),
            arguments(g.V().hasId(1).has("name", "marko"), g_V(T.id, 1, "name", eq("marko")), Collections.emptyList()),
            arguments(g.V().hasId(1).hasLabel("Person"), g_V(T.id, 1, "~label", eq("Person")), Collections.emptyList()),
            arguments(g.V().hasLabel("Person").has("lang", "java").order().by("name"),
                g_V("~label", eq("Person"), "lang", eq("java"), new HasStepFolder.OrderEntry("name", Order.asc)),
                Collections.emptyList()),
            arguments(g.V().hasLabel("Person").has("lang", "java").order().by(new ElementValueComparator("name",
                    Order.asc)),
                g_V("~label", eq("Person"), "lang", eq("java"), new HasStepFolder.OrderEntry("name", Order.asc)),
                Collections.emptyList()),
            // same as above, different order
            arguments(g.V().hasLabel("Person").has("lang", "java").order().by("name", Order.desc),
                g_V("~label", eq("Person"), "lang", eq("java"), new HasStepFolder.OrderEntry("name", Order.desc)),
                Collections.emptyList()),
            arguments(g.V().hasLabel("Person").has("lang", "java").order().by(new ElementValueComparator("name",
                    Order.asc)),
                g_V("~label", eq("Person"), "lang", eq("java"), new HasStepFolder.OrderEntry("name", Order.asc)),
                Collections.emptyList()),
            // if multiple order steps are specified in a row, only the last will be folded in because it overrides previous ordering
            arguments(g.V().hasLabel("Person").has("lang", "java").order().by("lang", Order.asc).order().by("name",
                Order.desc),
                g_V("~label", eq("Person"), "lang", eq("java"), new HasStepFolder.OrderEntry("name", Order.desc)),
                Collections.emptyList()),
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
            arguments(g.V().or(has("name", "marko"), has("lang", "java").order().by("name", Order.desc)),
                g_V(__.or(g_V("name", eq("marko")), g_V("lang", eq("java"), new HasStepFolder.OrderEntry("name",
                    Order.desc)))), Collections.emptyList()),
            arguments(g.V().or(has("name", "marko"), has("lang", "java").order().by("name", Order.desc)).order().by(
                "lang", Order.asc),
                g_V(__.or(g_V("name", eq("marko")), g_V("lang", eq("java"), new HasStepFolder.OrderEntry("name",
                    Order.desc))), new HasStepFolder.OrderEntry("lang", Order.asc)), Collections.emptyList()),
            arguments(g.V().or(__.has("name", "marko").has("age", 29), __.has("name", "vadas").has("age", 27)).as("x").select("x"),
                g_V(__.or(g_V("name", eq("marko"), "age", eq(29)), g_V("name", eq("vadas"), "age", eq(27)))).as("x").select("x"), Collections.emptyList()),
        });
    }

    private static Stream<Arguments> generateMultiQueryTestParameters() {
        final StandardJanusGraph graph = (StandardJanusGraph) StorageSetup.getInMemoryGraphWithMultiQuery();
        final GraphTraversalSource g = graph.traversal();
        // create a basic schema so that order pushdown can be tested as this optimization requires a JanusGraph
        // transaction registered against a non-EmptyGraph
        final JanusGraphManagement mgmt = graph.openManagement();
        mgmt.makePropertyKey("name").dataType(String.class).make();
        mgmt.makePropertyKey("lang").dataType(String.class).make();
        mgmt.makePropertyKey("weight").cardinality(Cardinality.SINGLE).dataType(Integer.class).make();
        mgmt.makeEdgeLabel("knows").make();
        mgmt.commit();

        // String constants for expected types of JanusGraphMultiQueryStep
        final String MQ_CHOOSE = "ChooseStep";
        final String MQ_UNION = "UnionStep";
        final String MQ_OPTIONAL = "OptionalStep";
        final String MQ_FILTER = "TraversalFilterStep";
        final String MQ_REPEAT = "RepeatEndStep";

        List<JanusGraphLocalQueryOptimizerStrategy> otherStrategies = Collections.singletonList(JanusGraphLocalQueryOptimizerStrategy.instance());

        return Arrays.stream(new Arguments[]{
            arguments(g.V().choose(__.inE("knows").has("weight", 0),__.inE("knows").has("weight", 1)),
                g_V().is(MQ_CHOOSE).choose(__.inE("knows").has("weight", 0),__.inE("knows").has("weight", 1)), otherStrategies),
            arguments(g.V().union(__.inE("knows").has("weight", 0),__.inE("knows").has("weight", 1)),
                g_V().is(MQ_UNION).union(__.inE("knows").has("weight", 0),__.inE("knows").has("weight", 1)), otherStrategies),
            arguments(g.V().outE().optional(__.inE("knows").has("weight", 0)),
                g_V().outE().is(MQ_OPTIONAL).optional(__.inE("knows").has("weight", 0)), otherStrategies),
            arguments(g.V().outE().filter(__.inE("knows").has("weight", 0)),
                g_V().outE().is(MQ_FILTER).filter(__.inE("knows").has("weight", 0)), otherStrategies),
            // The JanusGraphMultiQueryStep for repeat goes before the RepeatEndStep allowing it to feed its starts to the next iteration
            arguments(g.V().outE("knows").inV().repeat(__.outE("knows").inV().has("weight", 0)).times(10),
                g_V().outE("knows").inV().repeat(__.outE("knows").inV().has("weight", 0).is(MQ_REPEAT)).times(10), otherStrategies),
            // Choose does not have a child traversal of JanusGraphVertexStep so won't benefit from JanusGraphMultiQueryStep(ChooseStep)
            arguments(g.V().choose(has("weight", lt(3)), __.union(__.inE("knows").has("weight", 0),__.inE("knows").has("weight", 1))),
                g_V().choose(has("weight", lt(3)), __.is(MQ_UNION).union(__.inE("knows").has("weight", 0),__.inE("knows").has("weight", 1))), otherStrategies),
            // Choose now has a child traversal of JanusGraphVertexStep and so will benefit from JanusGraphMultiQueryStep(ChooseStep)
            arguments(g.V().choose(__.union(__.inE("knows").has("weight", 0),__.inE("knows").has("weight", 1)),__.inE("knows").has("weight", gt(2))),
                g_V().is(MQ_CHOOSE).choose(__.is(MQ_UNION).union(__.inE("knows").has("weight", 0),__.inE("knows").has("weight", 1)),__.inE("knows").has("weight", gt(2))), otherStrategies),
            // There are 'as' side effect steps preceding the JanusGraphVertexStep
            arguments(g.V().choose(has("weight", 0),__.as("true").inE("knows"),__.as("false").inE("knows")),
                g_V().is(MQ_CHOOSE).choose(has("weight", 0),__.as("true").inE("knows"),__.as("false").inE("knows")), otherStrategies),
            // There are 'sideEffect' and 'as' steps preceding the JanusGraphVertexStep
            arguments(g.V().choose(has("weight", 0),__.as("true").sideEffect(i -> {}).inE("knows"),__.as("false").sideEffect(i -> {}).inE("knows")),
                g_V().is(MQ_CHOOSE).choose(has("weight", 0),__.as("true").sideEffect(i -> {}).inE("knows"),__.as("false").sideEffect(i -> {}).inE("knows")), otherStrategies),
        });
    }
}
