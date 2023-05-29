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

package org.janusgraph;

import org.janusgraph.diskstorage.util.backpressure.SemaphoreQueryBackPressure;
import org.janusgraph.diskstorage.util.backpressure.PassAllQueryBackPressure;
import org.janusgraph.diskstorage.util.backpressure.QueryBackPressure;
import org.janusgraph.diskstorage.util.backpressure.SemaphoreProtectedReleaseQueryBackPressure;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static org.janusgraph.util.system.ExecuteUtil.gracefulExecutorServiceShutdown;

/**
 * Benchmark for different implementations of `QueryBackPressure`.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class BackPressureBenchmark {

    /**
     * How many parallel threads which will try to acquire and release queries when the backPressure is reached.
     */
    @Param({ "2000", "1000", "100", "10", "4", "2", "1" })
    int threads;

    /**
     * `QueryBackPressure` size (ignored for `passAllBackPressure` type).
     */
    @Param({ "50000", "10000", "1000", "100" })
    int backPressure;

    @Param({
        "semaphoreReleaseProtectedBackPressureWithReleasesAwait",
        "semaphoreReleaseProtectedBackPressureWithoutReleasesAwait",
        "semaphoreBackPressure",
        "passAllBackPressure"})
    String type;

    private QueryBackPressure queryBackPressure;
    private ExecutorService queriesAcquireService;
    private ExecutorService queriesReleaseService;
    private boolean closeBackPressure;
    private Semaphore acquireJobsSemaphore;

    @Setup(Level.Invocation)
    public void setup() {
        acquireJobsSemaphore = new Semaphore(0);
        queriesAcquireService = Executors.newFixedThreadPool(threads);
        queriesReleaseService = Executors.newFixedThreadPool(threads);
        switch (type){
            case "semaphoreReleaseProtectedBackPressureWithReleasesAwait": {
                queryBackPressure = new SemaphoreProtectedReleaseQueryBackPressure(backPressure);
                closeBackPressure = true;
                break;
            }
            case "semaphoreReleaseProtectedBackPressureWithoutReleasesAwait": {
                queryBackPressure = new SemaphoreProtectedReleaseQueryBackPressure(backPressure);
                closeBackPressure = false;
                break;
            }
            case "semaphoreBackPressure": {
                queryBackPressure = new SemaphoreQueryBackPressure(backPressure);
                closeBackPressure = false;
                break;
            }
            case "passAllBackPressure": {
                queryBackPressure = new PassAllQueryBackPressure();
                closeBackPressure = false;
                break;
            }
            default: throw new IllegalArgumentException("No implementation found to type = "+type);
        }

        for(int j=0; j<backPressure; j++){
            queryBackPressure.acquireBeforeQuery();
            acquireJobsSemaphore.release();
        }

        for(int i = 0; i< threads; i++){
            queriesAcquireService.submit(() -> {
                queryBackPressure.acquireBeforeQuery();
                acquireJobsSemaphore.release();
            });
        }
    }

    @Benchmark
    public void releaseBlocked() {
        for(int i = 0; i< threads; i++){
            queriesReleaseService.submit(() -> {
                try {
                    acquireJobsSemaphore.acquire();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                queryBackPressure.releaseAfterQuery();
            });
        }

        gracefulExecutorServiceShutdown(queriesReleaseService, Long.MAX_VALUE);

        if(closeBackPressure){
            queryBackPressure.close();
        }
    }

    @TearDown(Level.Invocation)
    public void clearResources() {
        gracefulExecutorServiceShutdown(queriesAcquireService, Long.MAX_VALUE);
        queryBackPressure.close();
        System.gc();
    }

}
