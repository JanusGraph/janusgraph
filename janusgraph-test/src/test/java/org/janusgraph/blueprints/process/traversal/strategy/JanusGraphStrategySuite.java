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

package org.janusgraph.blueprints.process.traversal.strategy;

import org.janusgraph.blueprints.process.traversal.strategy.optimization.JanusGraphStepStrategyTest;
import org.apache.tinkerpop.gremlin.AbstractGremlinSuite;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.RunnerBuilder;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class JanusGraphStrategySuite extends AbstractGremlinSuite {

    public JanusGraphStrategySuite(final Class<?> klass, final RunnerBuilder builder) throws InitializationError {

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
