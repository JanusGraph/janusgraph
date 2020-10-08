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
import org.janusgraph.graphdb.tinkerpop.optimize.strategy.AdjacentVertexIsOptimizerStrategy;
import org.janusgraph.graphdb.types.system.ImplicitKey;
import org.junit.jupiter.api.Test;

import static org.janusgraph.testutil.JanusGraphAssert.assertOptimization;
import static org.janusgraph.testutil.JanusGraphAssert.assertSameResultWithOptimizations;

/**
 * @author Florian Grieskamp (Florian.Grieskamp@gdata.de)
 */
public class AdjacentVertexIsOptimizerStrategyTest extends OptimizerStrategyTest {

    private static TraversalStrategy optimizer = AdjacentVertexIsOptimizerStrategy.instance();

    @Test
    public void testAll() {
        makeSampleGraph();

        // AdjacentVertexIsOptimizer outE/inE/bothE
        assertOptimization(g.V(sv[0]).outE("knows").has(ImplicitKey.ADJACENT_ID.name(), vs[50]).inV(),
            g.V(sv[0]).outE("knows").inV().is(vs[50]), optimizer);
        assertOptimization(g.V(sv[0]).inE("knows").has(ImplicitKey.ADJACENT_ID.name(), vs[50]).outV(),
            g.V(sv[0]).inE("knows").outV().is(vs[50]), optimizer);
        assertOptimization(g.V(sv[0]).bothE("knows").has(ImplicitKey.ADJACENT_ID.name(), vs[50]).otherV(),
            g.V(sv[0]).bothE("knows").otherV().is(vs[50]), optimizer);

        // AdjacentVertexIsOptimizer out/in/both
        assertOptimization(g.V(sv[0]).outE("knows").has(ImplicitKey.ADJACENT_ID.name(), vs[50]).inV(),
            g.V(sv[0]).out("knows").is(vs[50]), optimizer);
        assertOptimization(g.V(sv[0]).inE("knows").has(ImplicitKey.ADJACENT_ID.name(), vs[50]).outV(),
            g.V(sv[0]).in("knows").is(vs[50]), optimizer);
        assertOptimization(g.V(sv[0]).bothE("knows").has(ImplicitKey.ADJACENT_ID.name(), vs[50]).otherV(),
            g.V(sv[0]).both("knows").is(vs[50]), optimizer);

        // Result should stay the same
        assertSameResultWithOptimizations(g.V(sv[0]).as("v1").out("knows").is(vs[50]).as("v2").select("v1", "v2").by("id"),
            AdjacentVertexIsOptimizerStrategy.instance());
    }
}
