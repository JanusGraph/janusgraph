// Copyright 2022 JanusGraph Authors
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
package org.janusgraph;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalInterruptedException;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphException;
import org.janusgraph.core.JanusGraphFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class BerkeleyInterruptionTest {

    @Test
    public void interruptedEnvironmentShouldBeRestarted(@TempDir File dir) {
        try (JanusGraph graph = JanusGraphFactory.open("berkeleyje:" + dir.getAbsolutePath())) {
            assertThrows(JanusGraphException.class, () -> {
                Transaction tx = graph.tx();
                GraphTraversalSource gtx = tx.begin();

                gtx.addV().iterate();

                Thread.currentThread().interrupt();
                tx.commit();
            });

            // Retry until BerkeleyJE DB environment is reopened
            while (true) {
                try {
                    graph.traversal().addV().iterate();
                    break;
                } catch (TraversalInterruptedException ignored) {
                }
            }

            assertEquals(1, graph.traversal().V().count().next());
        }
    }
}
