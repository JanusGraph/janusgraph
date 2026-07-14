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

package org.janusgraph.diskstorage.keycolumnvalue.scan;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CompletableScanJobFutureTest {

    private StandardScanMetrics metrics;
    private CompletableFuture<ScanMetrics> delegate;
    private AtomicBoolean interrupted;

    @BeforeEach
    public void setUp() {
        metrics = new StandardScanMetrics();
        delegate = new CompletableFuture<>();
        interrupted = new AtomicBoolean(false);
    }

    @Test
    public void testGetIntermediateResultWhileRunning() throws ExecutionException {
        CompletableScanJobFuture future = new CompletableScanJobFuture(delegate, metrics);
        assertEquals(metrics, future.getIntermediateResult());
        assertFalse(future.isDone());
    }

    @Test
    public void testGetReturnsCompletedMetrics() throws ExecutionException, InterruptedException {
        CompletableScanJobFuture future = new CompletableScanJobFuture(delegate, metrics);
        delegate.complete(metrics);
        assertEquals(metrics, future.get());
        assertTrue(future.isDone());
        assertFalse(future.isCancelled());
    }

    @Test
    public void testGetIntermediateResultThrowsAfterFailure() {
        CompletableScanJobFuture future = new CompletableScanJobFuture(delegate, metrics);
        IllegalStateException jobFailure = new IllegalStateException("failure");
        delegate.completeExceptionally(jobFailure);
        ExecutionException e = assertThrows(ExecutionException.class, future::getIntermediateResult);
        assertEquals(jobFailure, e.getCause());
    }

    @Test
    public void testGetIntermediateResultReportsJobFailureEvenWhenCallerIsInterrupted() {
        CompletableScanJobFuture future = new CompletableScanJobFuture(delegate, metrics);
        IllegalStateException jobFailure = new IllegalStateException("failure");
        delegate.completeExceptionally(jobFailure);
        Thread.currentThread().interrupt();
        try {
            ExecutionException e = assertThrows(ExecutionException.class, future::getIntermediateResult);
            //The job failure must be reported as the cause and must not be masked by the caller's interruption
            assertEquals(jobFailure, e.getCause());
            //The caller's interrupt status must be preserved, not consumed
            assertTrue(Thread.currentThread().isInterrupted());
        } finally {
            //Clear the interrupt flag so it does not leak into other tests
            Thread.interrupted();
        }
    }

    @Test
    public void testCancelWithInterruptRunsInterruptAction() {
        CompletableScanJobFuture future = new CompletableScanJobFuture(delegate, metrics, () -> interrupted.set(true));
        assertTrue(future.cancel(true));
        assertTrue(future.isCancelled());
        assertTrue(interrupted.get());
    }

    @Test
    public void testCancelWithoutInterruptDoesNotRunInterruptAction() {
        CompletableScanJobFuture future = new CompletableScanJobFuture(delegate, metrics, () -> interrupted.set(true));
        assertTrue(future.cancel(false));
        assertTrue(future.isCancelled());
        assertFalse(interrupted.get());
    }

    @Test
    public void testCancelAfterCompletionDoesNotRunInterruptAction() {
        CompletableScanJobFuture future = new CompletableScanJobFuture(delegate, metrics, () -> interrupted.set(true));
        delegate.complete(metrics);
        assertFalse(future.cancel(true));
        assertFalse(future.isCancelled());
        assertFalse(interrupted.get());
    }

    @Test
    public void testGetIntermediateResultReturnsPartialMetricsAfterCancellation() throws ExecutionException {
        CompletableScanJobFuture future = new CompletableScanJobFuture(delegate, metrics, () -> interrupted.set(true));
        assertTrue(future.cancel(true));
        //A cancelled job is not a failed job: the partial progress must remain accessible
        assertEquals(metrics, future.getIntermediateResult());
    }

    @Test
    public void testGetIntermediateResultReturnsPartialMetricsAfterCancellationWithoutInterrupt() throws ExecutionException {
        CompletableScanJobFuture future = new CompletableScanJobFuture(delegate, metrics);
        assertTrue(future.cancel(false));
        assertEquals(metrics, future.getIntermediateResult());
    }
}
