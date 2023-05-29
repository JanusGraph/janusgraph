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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

import static org.janusgraph.util.system.ExecuteUtil.gracefulExecutorServiceShutdown;

/**
 * Query back pressure implementation which uses Semaphore to control back pressure and has protection
 * in place to not generate more `permits` than `backPressureLimit`.<br>
 *
 * This implementation is similar to {@link  SemaphoreQueryBackPressure } with the exception that `releaseAfterQuery`
 * calls are asynchronous (non-blocking) and protected against generating more `permits` than `backPressureLimit`.
 * This comes with additional overhead of using a separate thread to process any new `release` calls
 * which means that all calls to `releaseAfterQuery` will be processed in sequential order one by one.
 * The first time logic registers that an attempt to add a new permit could potentially result in a bigger amount of
 * `permits` than `backPressureLimit`, it logs a warning. Subsequent calls to `releaseAfterQuery` will not log such
 * warning anymore.
 */
public class SemaphoreProtectedReleaseQueryBackPressure implements QueryBackPressure{

    private static final Logger log = LoggerFactory.getLogger(SemaphoreProtectedReleaseQueryBackPressure.class);

    private final ExecutorService executorService = Executors.newSingleThreadExecutor();
    private final Runnable releaseNonBlocking;
    private final Semaphore semaphore;
    private volatile boolean hadWarningLogged;

    public SemaphoreProtectedReleaseQueryBackPressure(final int backPressureLimit) {
        this.semaphore = new Semaphore(backPressureLimit, true);
        this.releaseNonBlocking = () -> {
            // ensure we never add more permits than `backPressureLimit`
            // (even if `releaseAfterQuery()` is called more times than `acquireBeforeQuery()`);
            if(semaphore.availablePermits()<backPressureLimit){
                semaphore.release();
            } else if(!hadWarningLogged){
                log.warn("`releaseAfterQuery` is called more than once for some of the `acquireBeforeQuery` calls. " +
                    "This is a sign that the logic using this `QueryBackPressure` may not properly handle special " +
                    "(potentially exceptional) cases. {} will not trigger more releases than {}. This warning will " +
                        "be logged only once and it will be ignored for other `releaseAfterQuery` calls which attempt " +
                        "to add more permits than the configured limit.",
                    SemaphoreProtectedReleaseQueryBackPressure.class.getSimpleName(), backPressureLimit);
                hadWarningLogged = true;
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

    boolean hadWarningLogged(){
        return hadWarningLogged;
    }
}
