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

import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.NoneStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.NoOpBarrierStep;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.tinkerpop.optimize.strategy.AdjacentVertexHasUniquePropertyOptimizerStrategy;
import org.junit.jupiter.api.Test;

import static org.janusgraph.graphdb.JanusGraphBaseTest.option;
import static org.janusgraph.testutil.JanusGraphAssert.assertNumStep;
import static org.janusgraph.testutil.JanusGraphAssert.assertSameResultWithOptimizations;

/**
 * @author Florian Grieskamp (Florian.Grieskamp@gdata.de)
 */
public class AdjacentVertexHasUniquePropertyOptimizerStrategyTest extends OptimizerStrategyTest {

    private static TraversalStrategy optimizer = AdjacentVertexHasUniquePropertyOptimizerStrategy.instance();

    @Test
    public void testWithAndWithoutStrategy() {
        makeSampleGraph();
        assertNumStep(1, 0, g.V(sv[0]).out().has("uniqueId", 0), HasStep.class);
        assertNumStep(1, 1, graph.traversal().withoutStrategies(AdjacentVertexHasUniquePropertyOptimizerStrategy.class).V(sv[0]).out().has("uniqueId", 0), HasStep.class);
    }

    @Test
    public void testWithBackendAccess() {
        makeSampleGraph();

        // AdjacentVertexHasUniquePropertyOptimizer
        assertNumStep(1, 0, g.V(sv[0]).out().has("uniqueId", 0), HasStep.class);
        assertNumStep(1, 0, g.V(sv[0]).out().barrier(2500).has("uniqueId", 0), NoOpBarrierStep.class);
        assertNumStep(1, 0, g.V(sv[0]).outE().inV().has("uniqueId", 0), NoOpBarrierStep.class);
        assertNumStep(1, 0, g.V(sv[0]).outE().inV().has("uniqueId", 0), HasStep.class);
        assertNumStep(1, 0, g.V(sv[0]).outE().inV().barrier(2500).has("uniqueId", 0), NoOpBarrierStep.class);
        assertNumStep(1, 0, g.V(sv[0]).outE().barrier(2500).inV().has("uniqueId", 0), NoOpBarrierStep.class);
        assertNumStep(1, 1, g.V(sv[0]).barrier(2500).out().barrier(2500).has("uniqueId", 0), NoOpBarrierStep.class);
        assertNumStep(1, 0, g.V(sv[0]).out().barrier(2500).has("uniqueId", 0), HasStep.class);
        assertNumStep(1, 1, g.V(sv[0]).out().has("id", 0), HasStep.class);
        assertNumStep(1, 0, g.V(sv[0]).out().has("id", 0).has("uniqueId", 0), HasStep.class);
        assertNumStep(0, 0, g.V(sv[0]).out().has("uniqueId", 10000), HasStep.class);
        assertNumStep(0, 1, g.V(sv[0]).out().has("uniqueId", 10000), NoneStep.class);

        // ensure step labels are handled correctly
        assertSameResultWithOptimizations(g.V().as("v1").out().has("uniqueId", 0).as("v2").select("v1", "v2").by("uniqueId"),
            AdjacentVertexHasUniquePropertyOptimizerStrategy.instance());
    }

    @Test
    public void testWithoutBackendAccess() {
        clopen(option(GraphDatabaseConfiguration.OPTIMIZER_BACKEND_ACCESS), false);
        makeSampleGraph();

        // AdjacentVertexHasUniquePropertyOptimizer
        assertNumStep(1, 1, g.V(sv[0]).out().has("uniqueId", 0), HasStep.class);
        assertNumStep(1, 1, g.V(sv[0]).out().barrier(2500).has("uniqueId", 0), NoOpBarrierStep.class);
        assertNumStep(1, 0, g.V(sv[0]).outE().inV().has("uniqueId", 0), NoOpBarrierStep.class);
        assertNumStep(1, 1, g.V(sv[0]).outE().inV().has("uniqueId", 0), HasStep.class);
        assertNumStep(1, 1, g.V(sv[0]).outE().inV().barrier(2500).has("uniqueId", 0), NoOpBarrierStep.class);
        assertNumStep(1, 1, g.V(sv[0]).outE().barrier(2500).inV().has("uniqueId", 0), NoOpBarrierStep.class);
        assertNumStep(1, 2, g.V(sv[0]).barrier(2500).out().barrier(2500).has("uniqueId", 0), NoOpBarrierStep.class);
        assertNumStep(1, 1, g.V(sv[0]).out().barrier(2500).has("uniqueId", 0), HasStep.class);
        assertNumStep(1, 1, g.V(sv[0]).out().has("id", 0), HasStep.class);
        assertNumStep(1, 1, g.V(sv[0]).out().has("id", 0).has("uniqueId", 0), HasStep.class);
        assertNumStep(0, 1, g.V(sv[0]).out().has("uniqueId", 10000), HasStep.class);
        assertNumStep(0, 0, g.V(sv[0]).out().has("uniqueId", 10000), NoneStep.class);

        // ensure step labels are handled correctly
        assertSameResultWithOptimizations(g.V().as("v1").out().has("uniqueId", 0).as("v2").select("v1", "v2").by("uniqueId"),
            AdjacentVertexHasUniquePropertyOptimizerStrategy.instance());
    }
}
