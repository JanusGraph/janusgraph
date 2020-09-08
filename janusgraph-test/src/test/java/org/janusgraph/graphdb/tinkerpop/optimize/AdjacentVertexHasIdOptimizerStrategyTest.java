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
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.janusgraph.graphdb.tinkerpop.optimize.strategy.AdjacentVertexHasIdOptimizerStrategy;
import org.janusgraph.graphdb.types.system.ImplicitKey;
import org.junit.jupiter.api.Test;

import static org.janusgraph.testutil.JanusGraphAssert.assertOptimization;
import static org.janusgraph.testutil.JanusGraphAssert.assertSameResultWithOptimizations;

/**
 * @author Florian Grieskamp (Florian.Grieskamp@gdata.de)
 */
public class AdjacentVertexHasIdOptimizerStrategyTest extends OptimizerStrategyTest {

    private static TraversalStrategy optimizer = AdjacentVertexHasIdOptimizerStrategy.instance();

    @Test
    public void testAll() {
        makeSampleGraph();
        // AdjacentVertexHasIdOptimizer out/in/both
        assertOptimization(g.V(sv[0]).outE("knows").has(ImplicitKey.ADJACENT_ID.name(), vs[50]).inV(),
            g.V(sv[0]).out("knows").hasId(vs[50]), optimizer);
        assertOptimization(g.V(sv[0]).inE("knows").has(ImplicitKey.ADJACENT_ID.name(), vs[50]).outV(),
            g.V(sv[0]).in("knows").hasId(vs[50]), optimizer);
        assertOptimization(g.V(sv[0]).bothE("knows").has(ImplicitKey.ADJACENT_ID.name(), vs[50]).otherV(),
            g.V(sv[0]).both("knows").hasId(vs[50]), optimizer);

        // AdjacentVertexHasIdOptimizer outE/inE/bothE
        assertOptimization(g.V(sv[0]).outE("knows").has(ImplicitKey.ADJACENT_ID.name(), vs[50]).inV(),
            g.V(sv[0]).outE("knows").inV().hasId(vs[50]), optimizer);
        assertOptimization(g.V(sv[0]).inE("knows").has(ImplicitKey.ADJACENT_ID.name(), vs[50]).outV(),
            g.V(sv[0]).inE("knows").outV().hasId(vs[50]), optimizer);
        assertOptimization(g.V(sv[0]).bothE("knows").has(ImplicitKey.ADJACENT_ID.name(), vs[50]).otherV(),
            g.V(sv[0]).bothE("knows").otherV().hasId(vs[50]), optimizer);

        // Result should stay the same
        assertSameResultWithOptimizations(g.V(sv[0]).as("v1").out("knows").hasId(vs[50].id()).as("v2").select("v1", "v2").by("id"),
            AdjacentVertexHasIdOptimizerStrategy.instance());


        // neq should not be optimized
        assertOptimization(g.V(sv[0]).in().hasId(P.neq(vs[50])),
            g.V(sv[0]).in().hasId(P.neq(vs[50])), optimizer);

        int[] loop1 = {0}; // repeat starts from vertex with id 0 and goes in to the sv[0] vertex then loops back out to the vertex with the next id
        int[] loop2 = {0};
        GraphTraversal t1 = g.V(vs[0], vs[1], vs[2])
            .repeat(__.inE("knows")
                .has(ImplicitKey.ADJACENT_ID.name(), sv[0].id())
                .outV()
                //.outE("knows")
                //.inV()
                .out("knows") // TINKERPOP-2342
                .sideEffect(e -> loop1[0] = e.loops())
                .has("id", loop1[0]))
            .times(numV);
        GraphTraversal t2 = g.V(vs[0], vs[1], vs[2])
            .repeat(__.inE("knows")
                .outV()
                .hasId(sv[0].id())
                //.outE("knows")
                //.inV()
                .out("knows") // TINKERPOP-2342
                .sideEffect(e -> loop2[0] = e.loops())
                .has("id", loop2[0]))
            .times(numV);

        assertOptimization(t1, t2, optimizer);
    }
}
