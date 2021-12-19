// Copyright 2021 JanusGraph Authors
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

package org.janusgraph.diskstorage.keycolumnvalue.scan;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CompletedJobFutureTest {
    private StandardScanMetrics metrics;
    private CompletedJobFuture future;

    @BeforeEach
    public void setUp() {
        metrics = new StandardScanMetrics();
        future = new CompletedJobFuture(metrics);
    }

    @Test
    public void testGetIntermediateResult() {
        assertEquals(metrics, future.getIntermediateResult());
    }

    @Test
    public void testGet() throws ExecutionException, InterruptedException {
        assertEquals(metrics, future.get());
    }

    @Test
    public void testCancel() {
        assertFalse(future.cancel(true));
    }

    @Test
    public void testIsCancelled() {
        assertFalse(future.isCancelled());
    }

    @Test
    public void testIsDone() {
        assertTrue(future.isDone());
    }
}
