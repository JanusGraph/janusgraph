package org.janusgraph.blueprints.process.traversal.strategy;

import org.janusgraph.blueprints.process.traversal.strategy.optimization.JanusGraphStepStrategyTest;
import org.apache.tinkerpop.gremlin.AbstractGremlinSuite;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.RunnerBuilder;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class JanusStrategySuite extends AbstractGremlinSuite {

    public JanusStrategySuite(final Class<?> klass, final RunnerBuilder builder) throws InitializationError {

        super(klass, builder,
                new Class<?>[]{
                        JanusGraphStepStrategyTest.class
                }, new Class<?>[]{
                        JanusGraphStepStrategyTest.class
                },
                false,
                TraversalEngine.Type.STANDARD);
    }

}
