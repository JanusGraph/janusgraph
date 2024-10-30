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
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.DropStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.IsStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.OrStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.ElementMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.LabelStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertiesStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertyMapStep;
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
import org.janusgraph.StorageSetup;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.predicate.ConnectiveJanusPredicate;
import org.janusgraph.graphdb.query.JanusGraphPredicateUtils;
import org.janusgraph.graphdb.tinkerpop.optimize.step.HasStepFolder;
import org.janusgraph.graphdb.tinkerpop.optimize.step.JanusGraphDropStep;
import org.janusgraph.graphdb.tinkerpop.optimize.step.JanusGraphElementMapStep;
import org.janusgraph.graphdb.tinkerpop.optimize.step.JanusGraphHasStep;
import org.janusgraph.graphdb.tinkerpop.optimize.step.JanusGraphLabelStep;
import org.janusgraph.graphdb.tinkerpop.optimize.step.JanusGraphMultiQueryStep;
import org.janusgraph.graphdb.tinkerpop.optimize.step.JanusGraphNoOpBarrierVertexOnlyStep;
import org.janusgraph.graphdb.tinkerpop.optimize.step.JanusGraphPropertiesStep;
import org.janusgraph.graphdb.tinkerpop.optimize.step.JanusGraphPropertyMapStep;
import org.janusgraph.graphdb.tinkerpop.optimize.step.JanusGraphStep;
import org.janusgraph.graphdb.tinkerpop.optimize.step.JanusGraphVertexStep;
import org.janusgraph.graphdb.tinkerpop.optimize.strategy.JanusGraphHasStepStrategy;
import org.janusgraph.graphdb.tinkerpop.optimize.strategy.JanusGraphLocalQueryOptimizerStrategy;
import org.janusgraph.graphdb.tinkerpop.optimize.strategy.JanusGraphMultiQueryStrategy;
import org.janusgraph.graphdb.tinkerpop.optimize.strategy.JanusGraphStepStrategy;
import org.janusgraph.graphdb.tinkerpop.optimize.strategy.JanusGraphUnusedMultiQueryRemovalStrategy;
import org.janusgraph.graphdb.tinkerpop.optimize.strategy.MultiQueryDropStepStrategyMode;
import org.janusgraph.graphdb.tinkerpop.optimize.strategy.MultiQueryStrategyRepeatStepMode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
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
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.has;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.not;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.properties;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.values;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.DROP_STEP_BATCH_MODE;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.LIMITED_BATCH_SIZE;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.REPEAT_STEP_BATCH_MODE;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.USE_MULTIQUERY;
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

    @Test
    public void shouldTriggerHasContainerSplit(){
        final GraphTraversal.Admin<?, ?> traversal = new DefaultGraphTraversal<>();
        final JanusGraphStep<Vertex, Vertex> graphStep = new JanusGraphStep<>(new GraphStep<>(traversal, Vertex.class, true));
        graphStep.addHasContainer(new HasContainer("age", P.between("1", "123")));
        assertEquals(2, graphStep.getHasContainers().size());
    }

    @Test
    public void shouldTriggerLocalHasContainerSplit(){
        final GraphTraversal.Admin<?, ?> traversal = new DefaultGraphTraversal<>();
        final JanusGraphStep<Vertex, Vertex> graphStep = new JanusGraphStep<>(new GraphStep<>(traversal, Vertex.class, true));
        List<HasContainer> localHasContainers = graphStep.addLocalHasContainersSplittingAndPContainers(
            traversal.getParent(),
            Arrays.asList(new HasContainer("age", P.between("1", "123")), new HasContainer("age2", P.between("123", "234"))));
        assertEquals(4, localHasContainers.size());
    }

    @Test
    public void shouldTriggerLocalHasContainerConvert(){
        final GraphTraversal.Admin<?, ?> traversal = new DefaultGraphTraversal<>();
        final JanusGraphStep<Vertex, Vertex> graphStep = new JanusGraphStep<>(new GraphStep<>(traversal, Vertex.class, true));
        List<HasContainer> localHasContainers = graphStep.addLocalHasContainersConvertingAndPContainers(
            traversal.getParent(),
            Arrays.asList(new HasContainer("age", P.between("1", "123")), new HasContainer("age2", P.between("123", "234"))));
        assertEquals(2, localHasContainers.size());
    }

    private void applyMultiQueryTraversalSteps(Traversal.Admin<?,?> traversal) {
        TraversalHelper.getStepsOfAssignableClassRecursively(VertexStep.class, traversal).forEach(vertexStep -> {
            JanusGraphVertexStep janusGraphVertexStep = new JanusGraphVertexStep<>(vertexStep);
            TraversalHelper.replaceStep(vertexStep, janusGraphVertexStep, vertexStep.getTraversal());
            if (JanusGraphTraversalUtil.isEdgeReturnStep(janusGraphVertexStep)) {
                HasStepFolder.foldInHasContainer(janusGraphVertexStep, vertexStep.getTraversal(), vertexStep.getTraversal());
            }
        });
        TraversalHelper.getStepsOfAssignableClassRecursively(PropertiesStep.class, traversal).forEach(vertexStep -> {
            JanusGraphPropertiesStep janusGraphPropertiesStep = new JanusGraphPropertiesStep<>(vertexStep, true, true);
            TraversalHelper.replaceStep(vertexStep, janusGraphPropertiesStep, vertexStep.getTraversal());
        });
        TraversalHelper.getStepsOfAssignableClassRecursively(IsStep.class, traversal).forEach(isStep -> {
            Object expectedStep = isStep.getPredicate().getValue();
            if (expectedStep.equals(JanusGraphMultiQueryStep.class.getSimpleName())) {
                TraversalHelper.replaceStep(isStep, new JanusGraphMultiQueryStep(isStep.getTraversal(), false), isStep.getTraversal());
            } else if (expectedStep.equals(JanusGraphNoOpBarrierVertexOnlyStep.class.getSimpleName())) {
                TraversalHelper.replaceStep(isStep, new JanusGraphNoOpBarrierVertexOnlyStep(isStep.getTraversal(), LIMITED_BATCH_SIZE.getDefaultValue()), isStep.getTraversal());
            }
        });
        TraversalHelper.getStepsOfAssignableClassRecursively(HasStep.class, traversal).forEach(hasStep -> {
            JanusGraphHasStep janusGraphHasStep = new JanusGraphHasStep<>(hasStep);
            TraversalHelper.replaceStep(hasStep, janusGraphHasStep, hasStep.getTraversal());
        });
        TraversalHelper.getStepsOfAssignableClassRecursively(PropertyMapStep.class, traversal).forEach(propertyMapStep -> {
            JanusGraphPropertyMapStep janusGraphPropertyMapStep = new JanusGraphPropertyMapStep<>(propertyMapStep, true, true);
            TraversalHelper.replaceStep(propertyMapStep, janusGraphPropertyMapStep, propertyMapStep.getTraversal());
        });
        TraversalHelper.getStepsOfAssignableClassRecursively(ElementMapStep.class, traversal).forEach(elementMapStep -> {
            JanusGraphElementMapStep janusGraphElementMapStep = new JanusGraphElementMapStep<>(elementMapStep, true, true);
            TraversalHelper.replaceStep(elementMapStep, janusGraphElementMapStep, elementMapStep.getTraversal());
        });
        TraversalHelper.getStepsOfAssignableClassRecursively(LabelStep.class, traversal).forEach(labelStep -> {
            JanusGraphLabelStep janusGraphLabelStep = new JanusGraphLabelStep<>(labelStep);
            TraversalHelper.replaceStep(labelStep, janusGraphLabelStep, labelStep.getTraversal());
        });
        TraversalHelper.getStepsOfAssignableClassRecursively(DropStep.class, traversal).forEach(dropStep -> {
            JanusGraphDropStep janusGraphDropStep = new JanusGraphDropStep<>(dropStep);
            TraversalHelper.replaceStep(dropStep, janusGraphDropStep, dropStep.getTraversal());
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
            } else if (hasKeyValues[i] instanceof DefaultGraphTraversal && ((DefaultGraphTraversal) hasKeyValues[i]).getStartStep() instanceof OrStep){
                OrStep<?> orStep = (OrStep<?>) ((DefaultGraphTraversal) hasKeyValues[i]).getStartStep();
                for (final Traversal.Admin<?, ?> child : orStep.getLocalChildren()) {
                    final JanusGraphStep<Vertex, Vertex> localGraphStep = ((JanusGraphStep<Vertex, Vertex>) ((DefaultGraphTraversal) child).getStartStep());
                    graphStep.addLocalHasContainersConvertingAndPContainers(orStep, localGraphStep.getHasContainers());
                    localGraphStep.getOrders().forEach(orderEntry -> graphStep.localOrderBy(orStep, localGraphStep.getHasContainers(), orderEntry.key, orderEntry.order));
                    graphStep.setLocalLimit(orStep, localGraphStep.getHasContainers(), localGraphStep.getLowLimit(), localGraphStep.getHighLimit());
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
        final StandardJanusGraph generalGraph = (StandardJanusGraph) JanusGraphFactory.open(
            StorageSetup.getInMemoryConfiguration().set(USE_MULTIQUERY, true)
                .set(DROP_STEP_BATCH_MODE, MultiQueryDropStepStrategyMode.ALL.getConfigName()));;
        final StandardJanusGraph graphWithRepeatClosestParent = (StandardJanusGraph) JanusGraphFactory.open(
            StorageSetup.getInMemoryConfiguration().set(USE_MULTIQUERY, true)
                .set(REPEAT_STEP_BATCH_MODE, MultiQueryStrategyRepeatStepMode.CLOSEST_REPEAT_PARENT.getConfigName()));
        final StandardJanusGraph graphWithRepeatAllParents = (StandardJanusGraph) JanusGraphFactory.open(
            StorageSetup.getInMemoryConfiguration().set(USE_MULTIQUERY, true)
                .set(REPEAT_STEP_BATCH_MODE, MultiQueryStrategyRepeatStepMode.ALL_REPEAT_PARENTS.getConfigName()));
        final StandardJanusGraph graphWithRepeatStartsOnlyOfAllParents = (StandardJanusGraph) JanusGraphFactory.open(
            StorageSetup.getInMemoryConfiguration().set(USE_MULTIQUERY, true)
                .set(REPEAT_STEP_BATCH_MODE, MultiQueryStrategyRepeatStepMode.STARTS_ONLY_OF_ALL_REPEAT_PARENTS.getConfigName()));

        for(StandardJanusGraph graph : Arrays.asList(generalGraph, graphWithRepeatClosestParent, graphWithRepeatAllParents, graphWithRepeatStartsOnlyOfAllParents)){
            // create a basic schema so that order pushdown can be tested as this optimization requires a JanusGraph
            // transaction registered against a non-EmptyGraph
            final JanusGraphManagement mgmt = graph.openManagement();
            mgmt.makePropertyKey("name").dataType(String.class).make();
            mgmt.makePropertyKey("lang").dataType(String.class).make();
            mgmt.makePropertyKey("weight").cardinality(Cardinality.SINGLE).dataType(Integer.class).make();
            mgmt.makeEdgeLabel("knows").make();
            mgmt.commit();
        }

        final GraphTraversalSource g = generalGraph.traversal();
        final GraphTraversalSource gRepeatClosestParent = graphWithRepeatClosestParent.traversal();
        final GraphTraversalSource gRepeatAllParents = graphWithRepeatAllParents.traversal();
        final GraphTraversalSource gRepeatStartsOnlyOfAllParents = graphWithRepeatStartsOnlyOfAllParents.traversal();

        // String constant for expected JanusGraphMultiQueryStep
        final String MQ_STEP = JanusGraphMultiQueryStep.class.getSimpleName();
        final String NO_OP_B_V_STEP = JanusGraphNoOpBarrierVertexOnlyStep.class.getSimpleName();

        List<TraversalStrategy.ProviderOptimizationStrategy> otherStrategies = new ArrayList<>(4);
        otherStrategies.add(JanusGraphLocalQueryOptimizerStrategy.instance());
        otherStrategies.add(JanusGraphHasStepStrategy.instance());
        otherStrategies.add(JanusGraphMultiQueryStrategy.instance());
        otherStrategies.add(JanusGraphUnusedMultiQueryRemovalStrategy.instance());
        int defaultBarrierSize = GraphDatabaseConfiguration.LIMITED_BATCH_SIZE.getDefaultValue();

        return Arrays.stream(new Arguments[]{
            arguments(g.V().in("knows").out("knows"),
                g_V().is(MQ_STEP).is(NO_OP_B_V_STEP).in("knows").is(MQ_STEP).is(NO_OP_B_V_STEP).out("knows"), otherStrategies),
            arguments(g.V().in("knows").values("weight"),
                g_V().is(MQ_STEP).is(NO_OP_B_V_STEP).in("knows").is(MQ_STEP).is(NO_OP_B_V_STEP).values("weight"), otherStrategies),
            // Need two JanusGraphMultiQuerySteps, one for each sub query because caches are flushed when queried
            arguments(g.V().choose(__.inE("knows").has("weight", 0),__.inE("knows").has("weight", 1)),
                g_V().is(MQ_STEP).is(NO_OP_B_V_STEP).choose(__.inE("knows").has("weight", 0),__.inE("knows").has("weight", 1)), otherStrategies),
            arguments(g.V().union(__.inE("knows").has("weight", 0),__.inE("knows").has("weight", 1)),
                g_V().is(MQ_STEP).is(NO_OP_B_V_STEP).union(__.inE("knows").has("weight", 0),__.inE("knows").has("weight", 1)), otherStrategies),
            arguments(g.V().outE().optional(__.inE("knows").has("weight", 0)),
                g_V().is(MQ_STEP).is(NO_OP_B_V_STEP).outE().is(MQ_STEP).is(NO_OP_B_V_STEP).optional(__.inE("knows").has("weight", 0)), otherStrategies),
            arguments(g.V().outE().filter(__.inE("knows").has("weight", 0)),
                g_V().is(MQ_STEP).is(NO_OP_B_V_STEP).outE().is(MQ_STEP).is(NO_OP_B_V_STEP).filter(__.inE("knows").has("weight", 0)), otherStrategies),
            // An additional JanusGraphMultiQueryStep for repeat goes before the RepeatEndStep allowing it to feed its starts to the next iteration
            arguments(g.V().outE("knows").inV().repeat(__.outE("knows").inV().has("weight", 0)).times(10),
                g_V().is(MQ_STEP).is(NO_OP_B_V_STEP).outE("knows").inV().is(MQ_STEP).is(NO_OP_B_V_STEP).repeat(__.outE("knows").inV().is(MQ_STEP).is(NO_OP_B_V_STEP).has("weight", 0).is(MQ_STEP).is(NO_OP_B_V_STEP)).times(10), otherStrategies),
            // Choose does not have a child traversal of JanusGraphVertexStep so won't benefit from JanusGraphMultiQueryStep(ChooseStep)
            arguments(g.V().choose(has("weight", lt(3)), __.union(__.inE("knows").has("weight", 0),__.inE("knows").has("weight", 1))),
                g_V().is(MQ_STEP).is(NO_OP_B_V_STEP).choose(has("weight", lt(3)), __.union(__.inE("knows").has("weight", 0),__.inE("knows").has("weight", 1))), otherStrategies),
            // Choose now has a child traversal of JanusGraphVertexStep and so will benefit from JanusGraphMultiQueryStep(ChooseStep)
            arguments(g.V().choose(__.union(__.inE("knows").has("weight", 0),__.inE("knows").has("weight", 1)),__.inE("knows").has("weight", gt(2))),
                g_V().is(MQ_STEP).is(NO_OP_B_V_STEP).choose(__.union(__.inE("knows").has("weight", 0),__.inE("knows").has("weight", 1)),__.inE("knows").has("weight", gt(2))), otherStrategies),
            // There are 'as' side effect steps preceding the JanusGraphVertexStep
            arguments(g.V().choose(has("weight", 0),__.as("true").inE("knows"),__.as("false").inE("knows")),
                g_V().is(MQ_STEP).is(NO_OP_B_V_STEP).choose(has("weight", 0),__.as("true").inE("knows"),__.as("false").inE("knows")), otherStrategies),
            // There are 'sideEffect' and 'as' steps preceding the JanusGraphVertexStep
            arguments(g.V().choose(has("weight", 0),__.as("true").sideEffect(i -> {}).inE("knows"),__.as("false").sideEffect(i -> {}).inE("knows")),
                g_V().is(MQ_STEP).is(NO_OP_B_V_STEP).choose(has("weight", 0),__.as("true").sideEffect(i -> {}).inE("knows"),__.as("false").sideEffect(i -> {}).inE("knows")), otherStrategies),
            // Only the most outer eligible `JanusGraphMultiQueryStep` should be used for MultiQueryCompatible start steps
            arguments(g.V().and(__.inE("knows"), __.outE("knows")),
                g_V().is(MQ_STEP).is(NO_OP_B_V_STEP).and(__.inE("knows"), __.outE("knows")), otherStrategies),
            // `JanusGraphMultiQueryStep` should be used for filter step when at least one child is registered as a client of `JanusGraphMultiQueryStep`.
            arguments(g.V().where(__.out("knows").count().is(P.gte(5))),
                g_V().is(MQ_STEP).is(NO_OP_B_V_STEP).where(__.out("knows").count().is(P.gte(5))), otherStrategies),
            // `JanusGraphMultiQueryStep` should not be used for filter step when none of children is registered as a client of `JanusGraphMultiQueryStep`.
            arguments(g.V().where(__.count().is(P.gte(5))),
                g_V().where(__.count().is(P.gte(5))), otherStrategies),
            // Should include barrier steps with the default configured size by `LIMITED_BATCH_SIZE` option
            arguments(g.V().out().repeat(__.out()).emit(),
                g_V().is(MQ_STEP).is(NO_OP_B_V_STEP).out().is(MQ_STEP).is(NO_OP_B_V_STEP).repeat(__.out().is(MQ_STEP).is(NO_OP_B_V_STEP)).emit(), otherStrategies),
            // Should not use `JanusGraphMultiQueryStep` at the beginning of repeat child traversals when `emit` is used.
            arguments(g.V().out().emit().repeat(__.out()),
                g_V().is(MQ_STEP).is(NO_OP_B_V_STEP).out().is(MQ_STEP).is(NO_OP_B_V_STEP).emit().repeat(__.out().is(MQ_STEP).is(NO_OP_B_V_STEP)), otherStrategies),
            // Should not use `JanusGraphMultiQueryStep` at the beginning of repeat child traversals when non-true `emit` is used after `repeat`.
            arguments(g.V().out().repeat(__.out()).emit(__.in("knows")),
                g_V().is(MQ_STEP).is(NO_OP_B_V_STEP).out().is(MQ_STEP).is(NO_OP_B_V_STEP).repeat(__.out().is(MQ_STEP).is(NO_OP_B_V_STEP)).emit( __.in("knows")), otherStrategies),
            // Should not use `JanusGraphMultiQueryStep` at the beginning of repeat child traversals when non-true `emit` is used before `repeat`.
            arguments(g.V().out().emit(__.in("knows")).repeat(__.out()),
                g_V().is(MQ_STEP).is(NO_OP_B_V_STEP).out().is(MQ_STEP).is(NO_OP_B_V_STEP).emit( __.in("knows")).repeat(__.out().is(MQ_STEP).is(NO_OP_B_V_STEP)), otherStrategies),
            // Should not use `JanusGraphMultiQueryStep` at the beginning of repeat child traversals when `until` is used after `repeat`.
            arguments(g.V().out().repeat(__.out()).until(__.in("knows").count().is(P.gte(5))),
                g_V().is(MQ_STEP).is(NO_OP_B_V_STEP).out().is(MQ_STEP).is(NO_OP_B_V_STEP).repeat(__.out().is(MQ_STEP).is(NO_OP_B_V_STEP)).until(__.in("knows").count().is(P.gte(5))), otherStrategies),
            // Should not use `JanusGraphMultiQueryStep` at the beginning of repeat child traversals when `until` is used before `repeat`.
            arguments(g.V().out().until(__.in("knows").count().is(P.gte(5))).repeat(__.out()),
                g_V().is(MQ_STEP).is(NO_OP_B_V_STEP).out().is(MQ_STEP).is(NO_OP_B_V_STEP).until(__.in("knows").count().is(P.gte(5))).repeat(__.out().is(MQ_STEP).is(NO_OP_B_V_STEP)), otherStrategies),
            // Should not use `JanusGraphMultiQueryStep` at the beginning of repeat child traversals when `emit` is used before `repeat` and `until` is used after `repeat`.
            arguments(g.V().out().emit(__.in("knows")).repeat(__.out()).until(__.in("knows").count().is(P.gte(5))),
                g_V().is(MQ_STEP).is(NO_OP_B_V_STEP).out().is(MQ_STEP).is(NO_OP_B_V_STEP).emit(__.in("knows")).repeat(__.out().is(MQ_STEP).is(NO_OP_B_V_STEP)).until(__.in("knows").count().is(P.gte(5))), otherStrategies),
            // Should not use `JanusGraphMultiQueryStep` at the repeat child traversals when `emit` is used after `repeat` and `until` is used after `repeat`.
            arguments(g.V().out().repeat(__.out()).emit(__.in("knows")).until(__.in("knows").count().is(P.gte(5))),
                g_V().is(MQ_STEP).is(NO_OP_B_V_STEP).out().is(MQ_STEP).is(NO_OP_B_V_STEP).repeat(__.out().is(MQ_STEP).is(NO_OP_B_V_STEP)).emit(__.in("knows")).until(__.in("knows").count().is(P.gte(5))), otherStrategies),
            // Should not use `JanusGraphMultiQueryStep` at the beginning of repeat child traversals when `emit` is used after `repeat` and `until` is used before `repeat`.
            arguments(g.V().out().until(__.in("knows").count().is(P.gte(5))).repeat(__.out()).emit(__.in("knows")),
                g_V().is(MQ_STEP).is(NO_OP_B_V_STEP).out().is(MQ_STEP).is(NO_OP_B_V_STEP).until(__.in("knows").count().is(P.gte(5))).repeat(__.out().is(MQ_STEP).is(NO_OP_B_V_STEP)).emit(__.in("knows")), otherStrategies),
            // Should not use `JanusGraphMultiQueryStep` at the repeat child traversals when `emit` is used before `repeat` and `until` is used before `repeat`.
            arguments(g.V().out().until(__.in("knows").count().is(P.gte(5))).emit(__.in("knows")).repeat(__.out()),
                g_V().is(MQ_STEP).is(NO_OP_B_V_STEP).out().is(MQ_STEP).is(NO_OP_B_V_STEP).until(__.in("knows").count().is(P.gte(5))).emit(__.in("knows")).repeat(__.out().is(MQ_STEP).is(NO_OP_B_V_STEP)), otherStrategies),
            // Should not use `JanusGraphMultiQueryStep` at the beginning of repeat child traversals, but should use it for any other steps after the fist step.
            arguments(g.V().out().until(__.in("knows").in("knows").count().is(P.gte(5))).repeat(__.out().out()).emit(__.in("knows").in("knows")),
                g_V().is(MQ_STEP).is(NO_OP_B_V_STEP).out().is(MQ_STEP).is(NO_OP_B_V_STEP).until(__.in("knows").is(MQ_STEP).is(NO_OP_B_V_STEP).in("knows").count().is(P.gte(5))).repeat(__.out().is(MQ_STEP).is(NO_OP_B_V_STEP).out().is(MQ_STEP).is(NO_OP_B_V_STEP)).emit(__.in("knows").is(MQ_STEP).is(NO_OP_B_V_STEP).in("knows")), otherStrategies),
            // Should not use `JanusGraphMultiQueryStep` at the end of repeat child traversal and before repeat step if none of repeat child starts are MultiQueriable steps
            arguments(g.V().out().until(__.map(Traverser::get).in("knows").count().is(P.gte(5))).repeat(__.map(Traverser::get).out()).emit(__.map(Traverser::get).in("knows")),
                g_V().is(MQ_STEP).is(NO_OP_B_V_STEP).out().until(__.map(Traverser::get).is(MQ_STEP).is(NO_OP_B_V_STEP).in("knows").count().is(P.gte(5))).repeat(__.map(Traverser::get).is(MQ_STEP).is(NO_OP_B_V_STEP).out()).emit(__.map(Traverser::get).is(MQ_STEP).is(NO_OP_B_V_STEP).in("knows")), otherStrategies),
            // Needs one JanusGraphMultiQueryStep before executing `and` step with `MultiQueriable` steps
            arguments(g.V().and(__.inE("knows"), __.inE("knows")),
                g_V().is(MQ_STEP).is(NO_OP_B_V_STEP).and(__.inE("knows"), __.inE("knows")), otherStrategies),
            // Needs one JanusGraphMultiQueryStep before executing `or` step with `MultiQueriable` steps
            arguments(g.V().or(__.inE("knows"), __.inE("knows")),
                g_V().is(MQ_STEP).is(NO_OP_B_V_STEP).or(__.inE("knows"), __.inE("knows")), otherStrategies),
            // Should use start of the last outer parent step after the first repeat step as JanusGraphMultiQueryStep.
            arguments(g.V().union(__.repeat(__.out("knows")).emit()),
                g_V().is(MQ_STEP).is(NO_OP_B_V_STEP).union(__.repeat(__.out("knows").is(MQ_STEP).is(NO_OP_B_V_STEP)).emit()), otherStrategies),
            // Repeat step mode: CLOSEST_REPEAT_PARENT. Should use start of the closest outer parent step after the first repeat step as JanusGraphMultiQueryStep, but not the last outer start step of the most outer repeat step.
            arguments(gRepeatClosestParent.V().repeat(__.union(__.repeat(__.out("knows")).emit())).emit(),
                ((GraphTraversal<Vertex, Vertex>) g_V()).repeat(__.is(MQ_STEP).is(NO_OP_B_V_STEP).union(__.repeat(__.out("knows").is(MQ_STEP).is(NO_OP_B_V_STEP)).emit())).emit(), otherStrategies),
            // Repeat step mode: ALL_REPEAT_PARENTS. Should use starts and ends of all parent repeat steps as JanusGraphMultiQueryStep
            arguments(gRepeatAllParents.V().repeat(__.union(__.repeat(__.out("knows")).emit())).emit(),
                ((GraphTraversal<Vertex, Vertex>) g_V()).is(MQ_STEP).is(NO_OP_B_V_STEP).repeat(__.is(MQ_STEP).is(NO_OP_B_V_STEP).union(__.repeat(__.out("knows").is(MQ_STEP).is(NO_OP_B_V_STEP)).emit()).is(MQ_STEP).is(NO_OP_B_V_STEP)).emit(), otherStrategies),
            // Repeat step mode: STARTS_ONLY_OF_ALL_REPEAT_PARENTS. Should use starts of all parent repeat steps as JanusGraphMultiQueryStep
            arguments(gRepeatStartsOnlyOfAllParents.V().repeat(__.union(__.repeat(__.out("knows")).emit())).emit(),
                ((GraphTraversal<Vertex, Vertex>) g_V()).is(MQ_STEP).is(NO_OP_B_V_STEP).repeat(__.is(MQ_STEP).is(NO_OP_B_V_STEP).union(__.repeat(__.out("knows").is(MQ_STEP).is(NO_OP_B_V_STEP)).emit())).emit(), otherStrategies),
            // Need `JanusGraphMultiQuerySteps` before `has` step (we use `map` step here just to not fold has step containers into JanusGraphStep
            arguments(g.V().map(Traverser::get).has("weight", 0),
                g_V().map(Traverser::get).is(MQ_STEP).is(NO_OP_B_V_STEP).has("weight", 0), otherStrategies),
            // Need `JanusGraphMultiQuerySteps` before `has` step which is used after other steps
            arguments(g.V().out().has("weight", 0),
                g_V().is(MQ_STEP).is(NO_OP_B_V_STEP).out().is(MQ_STEP).is(NO_OP_B_V_STEP).has("weight", 0), otherStrategies),
            // Need `JanusGraphMultiQuerySteps` before `valueMap` step which is used after other steps
            arguments(g.V().valueMap("weight"),
                g_V().is(MQ_STEP).is(NO_OP_B_V_STEP).valueMap("weight"), otherStrategies),
            // Need `JanusGraphMultiQuerySteps` before `propertyMap` step which is used after other steps
            arguments(g.V().propertyMap("weight"),
                g_V().is(MQ_STEP).is(NO_OP_B_V_STEP).propertyMap("weight"), otherStrategies),
            // Need `JanusGraphMultiQuerySteps` before `elementMap` step which is used after other steps
            arguments(g.V().elementMap("weight"),
                g_V().is(MQ_STEP).is(NO_OP_B_V_STEP).elementMap("weight"), otherStrategies),
            // Need `JanusGraphMultiQuerySteps` before `values` step which is used after other steps
            arguments(g.V().values("weight"),
                g_V().is(MQ_STEP).is(NO_OP_B_V_STEP).values("weight"), otherStrategies),
            // Need `JanusGraphMultiQuerySteps` before `properties` step which is used after other steps
            arguments(g.V().properties("weight"),
                g_V().is(MQ_STEP).is(NO_OP_B_V_STEP).properties("weight"), otherStrategies),
            // Need `JanusGraphLabelStep` to use instead of `LabelStep`
            arguments(g.V().label(),
                g_V().is(MQ_STEP).is(NO_OP_B_V_STEP).label(), otherStrategies),
            // Need `JanusGraphDropStep` to use instead of `DropStep`
            arguments(g.V().drop(),
                g_V().is(MQ_STEP).is(NO_OP_B_V_STEP).drop(), otherStrategies),
        });
    }
}
