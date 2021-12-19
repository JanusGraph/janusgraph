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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class FailedJobFuture implements ScanJobFuture {
    private final Throwable cause;

    public FailedJobFuture(Throwable cause) {
        this.cause = cause;
    }

    @Override
    public ScanMetrics getIntermediateResult() throws ExecutionException {
        throw new ExecutionException(cause);
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return false;
    }

    @Override
    public boolean isCancelled() {
        return false;
    }

    @Override
    public boolean isDone() {
        return true;
    }

    @Override
    public ScanMetrics get() throws InterruptedException, ExecutionException {
        throw new ExecutionException(cause);
    }

    @Override
    public ScanMetrics get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        throw new ExecutionException(cause);
    }
}
