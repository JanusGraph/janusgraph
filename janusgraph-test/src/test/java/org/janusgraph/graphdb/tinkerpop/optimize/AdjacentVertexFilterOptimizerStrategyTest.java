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

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.TraversalFilterStep;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedFactory;
import org.janusgraph.graphdb.tinkerpop.optimize.step.JanusGraphVertexStep;
import org.janusgraph.graphdb.tinkerpop.optimize.strategy.AdjacentVertexFilterOptimizerStrategy;
import org.junit.jupiter.api.Test;

import static java.time.Duration.ofSeconds;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.inV;
import static org.janusgraph.testutil.JanusGraphAssert.assertNumStep;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author Florian Grieskamp (Florian.Grieskamp@gdata.de)
 */
public class AdjacentVertexFilterOptimizerStrategyTest extends OptimizerStrategyTest {

    @Test
    public void shouldOptimizeFilterStep() {
        makeSampleGraph();

        // is() optimization within filter
        assertNumStep(1, 1, g.V(sv[0]).outE("knows").filter(__.inV().is(vs[50])),
            JanusGraphVertexStep.class, TraversalFilterStep.class);
        assertNumStep(1, 1, g.V(sv[0]).outE("knows").filter(__.otherV().is(vs[50])),
            JanusGraphVertexStep.class, TraversalFilterStep.class);
        assertNumStep(1, 1, g.V(sv[0]).bothE("knows").filter(__.otherV().is(vs[50])),
            JanusGraphVertexStep.class, TraversalFilterStep.class);

        // hasId() optimization within filter
        assertNumStep(1, 1, g.V(sv[0]).outE("knows").filter(__.inV().hasId(vs[50].id())),
            JanusGraphVertexStep.class, TraversalFilterStep.class);
        assertNumStep(1, 1, g.V(sv[0]).outE("knows").filter(__.otherV().hasId(vs[50].id())),
            JanusGraphVertexStep.class, TraversalFilterStep.class);
        assertNumStep(1, 1, g.V(sv[0]).bothE("knows").filter(__.otherV().hasId(vs[50].id())),
            JanusGraphVertexStep.class, TraversalFilterStep.class);
    }

    @Test
    public void shouldNotOptimizeFilterStep() {
        makeSampleGraph();

        // is() optimization within filter
        assertNumStep(1, 2, g.V(sv[0]).bothE("knows").filter(__.inV().is(vs[50])),
            JanusGraphVertexStep.class, TraversalFilterStep.class);

        // hasId() optimization within filter
        assertNumStep(1, 2, g.V(sv[0]).bothE("knows").filter(__.inV().hasId(vs[50].id())),
            JanusGraphVertexStep.class, TraversalFilterStep.class);
    }

    @Test
    public void shouldNotStuck() {
        assertTimeoutPreemptively(ofSeconds(10), () -> {
            Vertex vertex = graph.traversal().addV().next();
            graph.traversal().withStrategies(AdjacentVertexFilterOptimizerStrategy.instance())
                 .V().outE().has("p", "v").filter(inV().is(vertex)).tryNext();
        });
    }

    @Test
    public void shouldNotFailedWithDetachedVertex() {
        g.addV("A").property("p", "1").as("a")
         .addV("A").property("p", "2").as("b")
         .addE("E").from("a").to("b")
         .iterate();
        g.tx().commit();
        g.withStrategies(AdjacentVertexFilterOptimizerStrategy.instance());

        Vertex v1 = g.V().has("p", "1").next();
        Vertex v2 = g.V().has("p", "2").next();
        v1 = DetachedFactory.detach(v1, false);
        v2 = DetachedFactory.detach(v2, false);

        assertTrue(g.V(v1).bothE("E").filter(__.otherV().is(v2)).hasNext());
        assertTrue(g.V(v1).bothE("E").filter(__.otherV().hasId(v2.id())).hasNext());
    }
}
