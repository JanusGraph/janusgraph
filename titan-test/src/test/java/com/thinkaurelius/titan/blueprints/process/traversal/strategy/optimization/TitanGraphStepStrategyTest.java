package com.thinkaurelius.titan.blueprints.process.traversal.strategy.optimization;

import com.thinkaurelius.titan.graphdb.tinkerpop.optimize.TitanGraphStep;
import org.apache.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import org.apache.tinkerpop.gremlin.process.IgnoreEngine;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TitanGraphStepStrategyTest extends AbstractGremlinProcessTest {

    @Test
    @IgnoreEngine(TraversalEngine.Type.COMPUTER)
    public void shouldFoldInHasContainers() {
        GraphTraversal.Admin traversal = g.V().has("name", "marko").asAdmin();
        assertEquals(2, traversal.getSteps().size());
        assertEquals(HasStep.class, traversal.getEndStep().getClass());
        traversal.applyStrategies();
        assertEquals(1, traversal.getSteps().size());
        assertEquals(TitanGraphStep.class, traversal.getStartStep().getClass());
        assertEquals(TitanGraphStep.class, traversal.getEndStep().getClass());
        assertEquals(1, ((TitanGraphStep) traversal.getStartStep()).getHasContainers().size());
        assertEquals("name", ((TitanGraphStep<?, ?>) traversal.getStartStep()).getHasContainers().get(0).getKey());
        assertEquals("marko", ((TitanGraphStep<?, ?>) traversal.getStartStep()).getHasContainers().get(0).getValue());
        ////
        traversal = g.V().has("name", "marko").has("age", P.gt(20)).asAdmin();
        traversal.applyStrategies();
        assertEquals(1, traversal.getSteps().size());
        assertEquals(TitanGraphStep.class, traversal.getStartStep().getClass());
        assertEquals(2, ((TitanGraphStep) traversal.getStartStep()).getHasContainers().size());
        ////
        traversal = g.V().has("name", "marko").out().has("name", "daniel").asAdmin();
        traversal.applyStrategies();
        assertEquals(3, traversal.getSteps().size());
        assertEquals(TitanGraphStep.class, traversal.getStartStep().getClass());
        assertEquals(1, ((TitanGraphStep) traversal.getStartStep()).getHasContainers().size());
        assertEquals("name", ((TitanGraphStep<?, ?>) traversal.getStartStep()).getHasContainers().get(0).getKey());
        assertEquals("marko", ((TitanGraphStep<?, ?>) traversal.getStartStep()).getHasContainers().get(0).getValue());
        assertEquals(HasStep.class, traversal.getEndStep().getClass());
        ////
        traversal = g.V().has("name", "marko").out().V().has("name", "daniel").asAdmin();
        traversal.applyStrategies();
        assertEquals(3, traversal.getSteps().size());
        assertEquals(TitanGraphStep.class, traversal.getStartStep().getClass());
        assertEquals(1, ((TitanGraphStep) traversal.getStartStep()).getHasContainers().size());
        assertEquals("name", ((TitanGraphStep<?, ?>) traversal.getStartStep()).getHasContainers().get(0).getKey());
        assertEquals("marko", ((TitanGraphStep<?, ?>) traversal.getStartStep()).getHasContainers().get(0).getValue());
        assertEquals(TitanGraphStep.class, traversal.getSteps().get(2).getClass());
        assertEquals(1, ((TitanGraphStep) traversal.getSteps().get(2)).getHasContainers().size());
        assertEquals("name", ((TitanGraphStep<?, ?>) traversal.getSteps().get(2)).getHasContainers().get(0).getKey());
        assertEquals("daniel", ((TitanGraphStep<?,?>) traversal.getSteps().get(2)).getHasContainers().get(0).getValue());
        assertEquals(TitanGraphStep.class, traversal.getEndStep().getClass());
    }

}
