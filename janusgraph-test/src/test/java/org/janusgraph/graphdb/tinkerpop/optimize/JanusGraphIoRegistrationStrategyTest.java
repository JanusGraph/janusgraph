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

package org.janusgraph.graphdb.tinkerpop.optimize;

import org.apache.tinkerpop.gremlin.process.traversal.IO;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.IoStep;
import org.janusgraph.graphdb.tinkerpop.JanusGraphIoRegistry;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class JanusGraphIoRegistrationStrategyTest extends OptimizerStrategyTest {

    @Test
    public void shouldAddJanusGraphRegistryForIoTraversal() {
        GraphTraversal<Object, Object> t = g.io("someNonExistingFile");
        t.asAdmin().applyStrategies();

        IoStep ioStep = (IoStep) t.asAdmin().getStartStep();
        assertEquals(JanusGraphIoRegistry.instance(),
            ioStep.getParameters().get(IO.registry, null).get(0));
    }
}
