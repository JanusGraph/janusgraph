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
import java.util.concurrent.Future;

public interface ScanJobFuture extends Future<ScanMetrics> {

    /**
     * Returns a set of potentially incomplete and still-changing metrics
     * for this job.  This is not guaranteed to be the same object as the
     * one returned by {@link #get()}, nor will the metrics visible through
     * the object returned by this method necessarily eventually converge
     * on the same values in the object returned by {@link #get()}, though
     * the implementation should attempt to provide both properties when
     * practical.
     * <p>
     * The metrics visible through the object returned by this method may
     * also change their values between reads.  In other words, this is not
     * necessarily an immutable snapshot.
     * <p>
     * If the job has failed and the implementation is capable of
     * quickly detecting that, then the implementation should throw an
     * {@code ExecutionException}.  Returning metrics in case of failure is
     * acceptable, but throwing an exception is preferred.
     *
     * @return metrics for a potentially still-running job
     * @throws ExecutionException if the job threw an exception
     */
    ScanMetrics getIntermediateResult() throws ExecutionException;
}

