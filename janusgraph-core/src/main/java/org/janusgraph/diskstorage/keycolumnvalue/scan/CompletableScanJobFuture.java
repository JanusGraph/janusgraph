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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A {@link ScanJobFuture} backed by a {@link CompletableFuture} for jobs which are not executed through the
 * scan framework but still report their progress through {@link ScanMetrics}.
 */
public class CompletableScanJobFuture implements ScanJobFuture {

    private final CompletableFuture<ScanMetrics> future;
    private final ScanMetrics intermediateMetrics;
    private final Runnable interruptAction;

    public CompletableScanJobFuture(CompletableFuture<ScanMetrics> future, ScanMetrics intermediateMetrics) {
        this(future, intermediateMetrics, null);
    }

    /**
     * @param interruptAction executed when the future is cancelled with {@code mayInterruptIfRunning} so that
     *                        the backing computation can be interrupted (may be null)
     */
    public CompletableScanJobFuture(CompletableFuture<ScanMetrics> future, ScanMetrics intermediateMetrics,
                                    Runnable interruptAction) {
        this.future = future;
        this.intermediateMetrics = intermediateMetrics;
        this.interruptAction = interruptAction;
    }

    @Override
    public ScanMetrics getIntermediateResult() throws ExecutionException {
        //Cancellation counts as exceptional completion of a CompletableFuture, but a cancelled job is not a
        //failed job: keep reporting the partial progress like other ScanJobFuture implementations do. A future
        //which completed exceptionally for a non-cancel reason can never transition to cancelled (and vice
        //versa), so this check is race-free. join() is used instead of get() so that retrieving the completion
        //exception cannot be preempted by an interrupt of the calling thread.
        if (future.isCompletedExceptionally() && !future.isCancelled()) {
            try {
                future.join();
            } catch (CompletionException e) {
                throw new ExecutionException(e.getCause() != null ? e.getCause() : e);
            }
        }
        return intermediateMetrics;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        boolean cancelled = future.cancel(mayInterruptIfRunning);
        if (cancelled && mayInterruptIfRunning && interruptAction != null) {
            interruptAction.run();
        }
        return cancelled;
    }

    @Override
    public boolean isCancelled() {
        return future.isCancelled();
    }

    @Override
    public boolean isDone() {
        return future.isDone();
    }

    @Override
    public ScanMetrics get() throws InterruptedException, ExecutionException {
        return future.get();
    }

    @Override
    public ScanMetrics get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return future.get(timeout, unit);
    }
}
