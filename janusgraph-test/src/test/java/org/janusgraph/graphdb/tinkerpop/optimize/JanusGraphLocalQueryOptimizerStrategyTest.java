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
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.LocalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderGlobalStep;
import org.janusgraph.graphdb.query.profile.QueryProfiler;
import org.janusgraph.graphdb.tinkerpop.optimize.step.JanusGraphPropertiesStep;
import org.janusgraph.graphdb.tinkerpop.optimize.step.JanusGraphStep;
import org.janusgraph.graphdb.tinkerpop.optimize.step.JanusGraphVertexStep;
import org.junit.jupiter.api.Test;

import static org.apache.tinkerpop.gremlin.process.traversal.Order.asc;
import static org.apache.tinkerpop.gremlin.process.traversal.Order.desc;
import static org.janusgraph.graphdb.JanusGraphBaseTest.option;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.BATCH_PROPERTY_PREFETCHING;
import static org.janusgraph.testutil.JanusGraphAssert.assertNumStep;
import static org.janusgraph.testutil.JanusGraphAssert.queryProfilerAnnotationIsPresent;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author Florian Grieskamp (Florian.Grieskamp@gdata.de)
 */
public class JanusGraphLocalQueryOptimizerStrategyTest extends OptimizerStrategyTest {

    @Test
    public void testDefaultConfiguration() {
        makeSampleGraph();
        //Edge
        assertNumStep(numV / 5, 1, g.V(sv[0]).outE("knows").has("weight", 1), JanusGraphVertexStep.class);
        assertNumStep(numV, 1, g.V(sv[0]).outE("knows"), JanusGraphVertexStep.class);
        assertNumStep(numV, 1, g.V(sv[0]).out("knows"), JanusGraphVertexStep.class);
        assertNumStep(10, 1, g.V(sv[0]).local(__.outE("knows").limit(10)), JanusGraphVertexStep.class);
        assertNumStep(10, 1, g.V(sv[0]).local(__.outE("knows").range(10, 20)), LocalStep.class);
        assertNumStep(numV, 2, g.V(sv[0]).outE("knows").order().by("weight", desc), JanusGraphVertexStep.class, OrderGlobalStep.class);
        // Ensure the LocalStep is dropped because the Order can be folded in the JanusGraphVertexStep which in turn
        // will allow JanusGraphLocalQueryOptimizationStrategy to drop the LocalStep as the local ordering will be
        // provided by the single JanusGraphVertex step
        assertNumStep(10, 0, g.V(sv[0]).local(__.outE("knows").order().by("weight", desc).limit(10)), LocalStep.class);
        assertNumStep(numV / 5, 2, g.V(sv[0]).outE("knows").has("weight", 1).order().by("weight", asc), JanusGraphVertexStep.class, OrderGlobalStep.class);
        assertNumStep(10, 0, g.V(sv[0]).local(__.outE("knows").has("weight", 1).order().by("weight", asc).limit(10)), LocalStep.class);
        // Note that for this test, the upper offset of the range will be folded into the JanusGraphVertexStep
        // by JanusGraphLocalQueryOptimizationStrategy, but not the lower offset. The RangeGlobalStep will in turn be kept
        // to enforce this lower bound and the LocalStep will be left as is as the local behavior will have not been
        // entirely subsumed by the JanusGraphVertexStep
        assertNumStep(5, 1, g.V(sv[0]).local(__.outE("knows").has("weight", 1).has("weight", 1).order().by("weight", asc).range(10, 15)), LocalStep.class);

        //Property
        assertNumStep(numV / 5, 1, g.V(sv[0]).properties("names").has("weight", 1), JanusGraphPropertiesStep.class);
        assertNumStep(numV, 1, g.V(sv[0]).properties("names"), JanusGraphPropertiesStep.class);
        assertNumStep(10, 0, g.V(sv[0]).local(__.properties("names").order().by("weight", desc).limit(10)), LocalStep.class);
        assertNumStep(numV, 2, g.V(sv[0]).outE("knows").values("weight"), JanusGraphVertexStep.class, JanusGraphPropertiesStep.class);


        //Global graph queries
        assertNumStep(1, 1, g.V().has("id", numV / 5), JanusGraphStep.class);
        assertNumStep(1, 1, g.V().has("id", numV / 5).has("weight", (numV / 5) % 5), JanusGraphStep.class);
        assertNumStep(numV / 5, 1, g.V().has("weight", 1), JanusGraphStep.class);
        assertNumStep(10, 1, g.V().has("weight", 1).range(0, 10), JanusGraphStep.class);

        assertNumStep(superV, 1, g.V().has("id", sid), JanusGraphStep.class);
        //Ensure that as steps don't interfere
        assertNumStep(1, 1, g.V().has("id", numV / 5).as("x"), JanusGraphStep.class);
        assertNumStep(1, 1, g.V().has("id", numV / 5).has("weight", (numV / 5) % 5).as("x"), JanusGraphStep.class);


        assertNumStep(superV * (numV / 5), 2, g.V().has("id", sid).outE("knows").has("weight", 1), JanusGraphStep.class, JanusGraphVertexStep.class);
        assertNumStep(superV * (numV / 5 * 2), 2, g.V().has("id", sid).outE("knows").has("weight", P.gte(1)).has("weight", P.lt(3)), JanusGraphStep.class, JanusGraphVertexStep.class);
        assertNumStep(superV * (numV / 5 * 2), 2, g.V().has("id", sid).outE("knows").has("weight", P.between(1, 3)), JanusGraphStep.class, JanusGraphVertexStep.class);
        assertNumStep(superV * 10, 2, g.V().has("id", sid).local(__.outE("knows").has("weight", P.gte(1)).has("weight", P.lt(3)).limit(10)), JanusGraphStep.class, JanusGraphVertexStep.class);
        assertNumStep(superV * 10, 1, g.V().has("id", sid).local(__.outE("knows").has("weight", P.between(1, 3)).order().by("weight", desc).limit(10)), JanusGraphStep.class);
        assertNumStep(superV * 10, 0, g.V().has("id", sid).local(__.outE("knows").has("weight", P.between(1, 3)).order().by("weight", desc).limit(10)), LocalStep.class);

        // Verify that the batch property pre-fetching is not applied when the configuration option is not set
        Traversal t = g.V().has("id", sid).outE("knows").has("weight", P.between(1, 3)).inV().has("weight", P.between(1, 3)).profile("~metrics");
        assertNumStep(superV * (numV / 5 * 2), 2, (GraphTraversal) t, JanusGraphStep.class, JanusGraphVertexStep.class);
        assertFalse(queryProfilerAnnotationIsPresent(t, QueryProfiler.MULTIPREFETCH_ANNOTATION));
    }

    @Test
    public void testBatchPropertyPrefetching() {
        clopen(option(BATCH_PROPERTY_PREFETCHING), true);
        makeSampleGraph();

        // This tests an edge property before inV and will trigger the multiQuery property pre-fetch optimisation in JanusGraphEdgeVertexStep
        Traversal t = g.V().has("id", sid).outE("knows").has("weight", P.between(1, 3)).inV().has("weight", P.between(1, 3)).profile("~metrics");
        assertNumStep(superV * (numV / 5 * 2), 2, (GraphTraversal)t, JanusGraphStep.class, JanusGraphVertexStep.class);
        assertTrue(queryProfilerAnnotationIsPresent(t, QueryProfiler.MULTIPREFETCH_ANNOTATION));

        // This tests a vertex property after inV and will trigger the multiQuery property pre-fetch optimisation in JanusGraphVertexStep
        t = g.V().has("id", sid).outE("knows").inV().has("weight", P.between(1, 3)).profile("~metrics");
        assertNumStep(superV * (numV / 5 * 2), 2, (GraphTraversal)t, JanusGraphStep.class, JanusGraphVertexStep.class);
        assertTrue(queryProfilerAnnotationIsPresent(t, QueryProfiler.MULTIPREFETCH_ANNOTATION));

        // As above but with a limit after the has step meaning property pre-fetch won't know how much to fetch and so should not be used
        t = g.V().has("id", sid).outE("knows").inV().has("weight", P.between(1, 3)).limit(1000).profile("~metrics");
        assertNumStep(superV * (numV / 5 * 2), 2, (GraphTraversal)t, JanusGraphStep.class, JanusGraphVertexStep.class);
        assertFalse(queryProfilerAnnotationIsPresent(t, QueryProfiler.MULTIPREFETCH_ANNOTATION));
    }

}
