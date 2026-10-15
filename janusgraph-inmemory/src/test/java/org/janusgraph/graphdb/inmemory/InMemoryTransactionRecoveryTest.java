// Copyright 2026 JanusGraph Authors
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

package org.janusgraph.graphdb.inmemory;

import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.log.TransactionRecovery;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class InMemoryTransactionRecoveryTest {

    //Transaction recovery times tx.max-commit-time in nanoseconds and waits about 146 years at most. A longer value is
    //a Duration like any other and must not keep a recovery process from starting: it waits up to that limit.
    @Test
    public void shouldStartTransactionRecoveryWithAMaxCommitTimeLongerThanNanosecondsHold() {
        final JanusGraph graph = JanusGraphFactory.build()
            .set("storage.backend", "inmemory")
            .set("tx.log-tx", true)
            .set("tx.max-commit-time", Duration.ofDays(365L * 1000))
            .open();
        try {
            //Read the log from now: a start in the past makes recovery read every 100 s slice of the log since then
            final TransactionRecovery recovery = assertDoesNotThrow(
                () -> JanusGraphFactory.startTransactionRecovery(graph, Instant.now()));
            try {
                assertEquals(Duration.ofDays(365L * 1000), ((StandardJanusGraph) graph).getConfiguration().getMaxCommitTime());
            } finally {
                recovery.shutdown();
            }
        } finally {
            graph.close();
        }
    }
}
