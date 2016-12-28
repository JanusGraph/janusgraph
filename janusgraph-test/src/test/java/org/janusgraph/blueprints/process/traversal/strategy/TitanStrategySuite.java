package com.thinkaurelius.titan.blueprints.process.traversal.strategy;

import com.thinkaurelius.titan.blueprints.process.traversal.strategy.optimization.TitanGraphStepStrategyTest;
import org.apache.tinkerpop.gremlin.AbstractGremlinSuite;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.RunnerBuilder;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TitanStrategySuite extends AbstractGremlinSuite {

    public TitanStrategySuite(final Class<?> klass, final RunnerBuilder builder) throws InitializationError {

        super(klass, builder,
                new Class<?>[]{
                        TitanGraphStepStrategyTest.class
                }, new Class<?>[]{
                        TitanGraphStepStrategyTest.class
                },
                false,
                TraversalEngine.Type.STANDARD);
    }

}