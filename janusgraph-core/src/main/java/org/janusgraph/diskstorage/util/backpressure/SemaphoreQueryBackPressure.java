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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

import static org.janusgraph.util.system.ExecuteUtil.gracefulExecutorServiceShutdown;

public class SemaphoreQueryBackPressure implements QueryBackPressure{

    private final ExecutorService executorService = Executors.newSingleThreadExecutor();
    private final Runnable releaseNonBlocking;
    private final Semaphore semaphore;

    public SemaphoreQueryBackPressure(final int backPressureLimit) {
        this.semaphore = new Semaphore(backPressureLimit, true);
        this.releaseNonBlocking = () -> {
            // ensure we never add more permits than `backPressureLimit`
            // (even if `releaseAfterQuery()` is called more times than `acquireBeforeQuery()`);
            if(semaphore.availablePermits()<backPressureLimit){
                semaphore.release();
            }
        };
    }

    @Override
    public void acquireBeforeQuery() {
        try {
            semaphore.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new JanusGraphException(e);
        }
    }

    @Override
    public void releaseAfterQuery(){
        executorService.execute(releaseNonBlocking);
    }

    @Override
    public void close() {
        gracefulExecutorServiceShutdown(executorService, Long.MAX_VALUE);
    }

    int availablePermits(){
        return semaphore.availablePermits();
    }
}
