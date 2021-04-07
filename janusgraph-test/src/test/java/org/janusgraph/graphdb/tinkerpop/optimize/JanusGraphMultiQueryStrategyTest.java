// Copyright 2020 JanusGraph Authors
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

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.ChooseStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.LocalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.OptionalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.UnionStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.TraversalFilterStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.NoOpBarrierStep;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.janusgraph.graphdb.query.profile.QueryProfiler;
import org.janusgraph.graphdb.tinkerpop.optimize.step.JanusGraphMultiQueryStep;
import org.janusgraph.graphdb.tinkerpop.optimize.step.JanusGraphPropertiesStep;
import org.janusgraph.graphdb.tinkerpop.optimize.step.JanusGraphStep;
import org.janusgraph.graphdb.tinkerpop.optimize.step.JanusGraphVertexStep;
import org.janusgraph.graphdb.tinkerpop.optimize.strategy.JanusGraphLocalQueryOptimizerStrategy;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.apache.tinkerpop.gremlin.process.traversal.Order.desc;
import static org.janusgraph.graphdb.JanusGraphBaseTest.option;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.LIMIT_BATCH_SIZE;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.USE_MULTIQUERY;
import static org.janusgraph.testutil.JanusGraphAssert.assertNumStep;
import static org.janusgraph.testutil.JanusGraphAssert.assertCount;
import static org.janusgraph.testutil.JanusGraphAssert.queryProfilerAnnotationIsPresent;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class JanusGraphMultiQueryStrategyTest extends OptimizerStrategyTest {

    @Test
    public void testQueryIsExecutableIfJanusGraphLocalQueryOptimizerStrategyIsEnabled() {
        clopen(option(USE_MULTIQUERY), true);
        makeSampleGraph();

        final List<Edge> normalResults = g.V(sv[0]).outE().inV().choose(__.inE("knows").has("weight", 0), __.inE("knows").has("weight", 1), __.inE("knows").has("weight", 2)).toList();
        final List<Edge> resultsWithDisabledStrategy = g.withoutStrategies(JanusGraphLocalQueryOptimizerStrategy.class).V(sv[0]).outE().inV().choose(__.inE("knows").has("weight", 0), __.inE("knows").has("weight", 1), __.inE("knows").has("weight", 2)).toList();

        assertEquals(normalResults, resultsWithDisabledStrategy);
    }

    @Test
    public void testNoMultiQueryStepsInsertedIfPathQuery() {
        clopen(option(USE_MULTIQUERY), true);
        makeSampleGraph();

        final GraphTraversal<?,?> traversalWithoutPath = g.V(sv[0]).outE().inV();
        assertNumStep(numV, 1, traversalWithoutPath, JanusGraphMultiQueryStep.class);

        final GraphTraversal<?,?> traversalWithPath = g.V(sv[0]).outE().inV().path();
        assertNumStep(numV, 0, traversalWithPath, JanusGraphMultiQueryStep.class);

        final GraphTraversal<?,?> traversalWithNestedPath = g.V(sv[0]).outE().inV().where(__.path());
        assertNumStep(numV, 0, traversalWithNestedPath, JanusGraphMultiQueryStep.class);
    }

    @Test
    public void testNoOpBarrierStepInsertedIfNotPresentAndLimitBatchSize() {
        clopen(option(USE_MULTIQUERY), true, option(LIMIT_BATCH_SIZE), true);
        makeSampleGraph();

        final GraphTraversal<?,?> traversalWithoutExplicitBarrier = g.V(sv[0]).outE().inV();
        assertNumStep(numV, 1, traversalWithoutExplicitBarrier, NoOpBarrierStep.class);

        final GraphTraversal<?,?> traversalWithExplicitBarrier = g.V(sv[0]).barrier(1).outE().inV();
        assertNumStep(numV, 1, traversalWithExplicitBarrier, NoOpBarrierStep.class);
    }

    @Test
    public void testNoOpBarrierStepNotInsertedLimitBatchSizeDisabled() {
        clopen(option(USE_MULTIQUERY), true, option(LIMIT_BATCH_SIZE), false);
        makeSampleGraph();

        final GraphTraversal<?,?> traversalWithoutExplicitBarrier = g.V(sv[0]).outE().inV();
        assertNumStep(numV, 0, traversalWithoutExplicitBarrier, NoOpBarrierStep.class);

        final GraphTraversal<?,?> traversalWithExplicitBarrier = g.V(sv[0]).barrier(1).outE().inV();
        assertNumStep(numV, 1, traversalWithExplicitBarrier, NoOpBarrierStep.class);
    }

    @Test
    public void testMultiQuery() {
        clopen(option(USE_MULTIQUERY), true);
        makeSampleGraph();

        Traversal t = g.V(sv[0]).outE().inV().choose(__.inE("knows").has("weight", 0),__.inE("knows").has("weight", 1), __.inE("knows").has("weight", 2)).profile("~metrics");
        assertNumStep(numV * 2, 2, (GraphTraversal)t, ChooseStep.class, JanusGraphVertexStep.class);
        assertTrue(queryProfilerAnnotationIsPresent(t, QueryProfiler.MULTIQUERY_ANNOTATION));

        t = g.V(sv[0]).outE().inV().union(__.inE("knows").has("weight", 0),__.inE("knows").has("weight", 1),__.inE("knows").has("weight", 2)).profile("~metrics");
        assertNumStep(numV * 6, 2, (GraphTraversal)t, UnionStep.class, JanusGraphVertexStep.class);
        assertTrue(queryProfilerAnnotationIsPresent(t, QueryProfiler.MULTIQUERY_ANNOTATION));

        int[] loop = {0}; // repeat starts from vertex with id 0 and goes in to the sv[0] vertex then loops back out to the vertex with the next id
        t = g.V(vs[0], vs[1], vs[2])
            .repeat(__.inE("knows")
                .outV()
                .hasId(sv[0].id())
                .out("knows") // TINKERPOP-2342
                .sideEffect(e -> loop[0] = e.loops())
                .has("id", loop[0]))
            .times(numV)
            .profile("~metrics");
        assertNumStep(3, 1, (GraphTraversal)t, RepeatStep.class);
        assertEquals(numV - 1, loop[0]);
        assertTrue(queryProfilerAnnotationIsPresent(t, QueryProfiler.MULTIQUERY_ANNOTATION));

        t = g.V(vs[0],vs[1],vs[2]).optional(__.inE("knows").has("weight", 0)).profile("~metrics");
        assertNumStep(12, 1, (GraphTraversal)t, OptionalStep.class);
        assertTrue(queryProfilerAnnotationIsPresent(t, QueryProfiler.MULTIQUERY_ANNOTATION));

        t = g.V(vs[0],vs[1],vs[2]).filter(__.inE("knows").has("weight", 0)).profile("~metrics");
        assertNumStep(1, 1, (GraphTraversal)t, TraversalFilterStep.class);
        assertTrue(queryProfilerAnnotationIsPresent(t, QueryProfiler.MULTIQUERY_ANNOTATION));

        assertNumStep(superV * (numV / 5), 2, g.V().has("id", sid).outE("knows").has("weight", 1), JanusGraphStep.class, JanusGraphVertexStep.class);
        assertNumStep(superV * (numV / 5 * 2), 2, g.V().has("id", sid).outE("knows").has("weight", P.between(1, 3)), JanusGraphStep.class, JanusGraphVertexStep.class);
        assertNumStep(superV * 10, 2, g.V().has("id", sid).local(__.outE("knows").has("weight", P.gte(1)).has("weight", P.lt(3)).limit(10)), JanusGraphStep.class, JanusGraphVertexStep.class);
        assertNumStep(superV * 10, 1, g.V().has("id", sid).local(__.outE("knows").has("weight", P.between(1, 3)).order().by("weight", desc).limit(10)), JanusGraphStep.class);
        assertNumStep(superV * 10, 0, g.V().has("id", sid).local(__.outE("knows").has("weight", P.between(1, 3)).order().by("weight", desc).limit(10)), LocalStep.class);
        assertNumStep(superV * numV, 2, g.V().has("id", sid).values("names"), JanusGraphStep.class, JanusGraphPropertiesStep.class);

        //Verify traversal metrics when all reads are from cache (i.e. no backend queries)
        t = g.V().has("id", sid).local(__.outE("knows").has("weight", P.between(1, 3)).order().by("weight", desc).limit(10)).profile("~metrics");
        assertCount(superV * 10, t);
        assertTrue(queryProfilerAnnotationIsPresent(t, QueryProfiler.MULTIQUERY_ANNOTATION));

        //Verify that properties also use multi query
        t = g.V().has("id", sid).values("names").profile("~metrics");
        assertCount(superV * numV, t);
        assertTrue(queryProfilerAnnotationIsPresent(t, QueryProfiler.MULTIQUERY_ANNOTATION));
    }
}
