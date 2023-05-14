// Copyright 2023 JanusGraph Authors
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

package org.janusgraph.diskstorage.util.backpressure;

import org.janusgraph.core.JanusGraphException;

import java.util.concurrent.Semaphore;

/**
 * Query back pressure implementation which uses Semaphore to control back pressure.<br>
 *
 * Warning: This implementation assumes that for each `acquireBeforeQuery` call there will be exactly
 * one `releaseAfterQuery` call. This implementation uses `backPressureLimit` as a starting `permits` amount
 * of the Semaphore. Each time `releaseAfterQuery` is called it will add a new `permit` even if the
 * current total amount of permits is already grater then `backPressureLimit`.
 * In case you assume that the logic where `SemaphoreQueryBackPressure` is used might be affected by
 * any bug which may call `releaseAfterQuery` more than once for a single `acquireBeforeQuery` call then
 * it's suggested to use {@link  SemaphoreProtectedReleaseQueryBackPressure } which has a tiny overhead
 * for `releaseAfterQuery` calls but protects those calls from the possible side effects of calling
 * `releaseAfterQuery` more than once for any `acquireBeforeQuery` call.
 */
public class SemaphoreQueryBackPressure extends Semaphore implements QueryBackPressure{

    public SemaphoreQueryBackPressure(final int backPressureLimit) {
        super(backPressureLimit, true);
    }

    @Override
    public void acquireBeforeQuery() {
        try {
            acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new JanusGraphException(e);
        }
    }

    @Override
    public void releaseAfterQuery(){
        release();
    }

    @Override
    public void close() {
        // ignored
    }
}
