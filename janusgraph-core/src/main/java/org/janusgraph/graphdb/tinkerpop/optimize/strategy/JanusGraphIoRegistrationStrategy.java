// Copyright 2019 JanusGraph Authors
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

package org.janusgraph.graphdb.tinkerpop.optimize.strategy;

import org.apache.tinkerpop.gremlin.process.traversal.IO;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.IoStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.janusgraph.graphdb.tinkerpop.JanusGraphIoRegistry;

/**
 * JanusGraphIoRegistrationStrategy registers the {@link JanusGraphIoRegistry} for the {@link IoStep}:
 */
public class JanusGraphIoRegistrationStrategy extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy>
    implements TraversalStrategy.ProviderOptimizationStrategy {

    private static final JanusGraphIoRegistrationStrategy INSTANCE = new JanusGraphIoRegistrationStrategy();

    @Override
    public void apply(Traversal.Admin<?, ?> traversal) {
        if (traversal.getStartStep() instanceof IoStep) {
            IoStep ioStep = (IoStep) traversal.getStartStep();
            ioStep.configure(IO.registry, JanusGraphIoRegistry.instance());
        }
    }

    public static JanusGraphIoRegistrationStrategy instance() {
        return INSTANCE;
    }
}
