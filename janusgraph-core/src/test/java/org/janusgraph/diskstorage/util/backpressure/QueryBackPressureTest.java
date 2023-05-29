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

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.janusgraph.util.system.ExecuteUtil.gracefulExecutorServiceShutdown;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class QueryBackPressureTest {

    @Test
    public void testNoExtraPermitsAreAddedWithSemaphoreProtectedReleaseQueryBackPressure() throws InterruptedException {
        SemaphoreProtectedReleaseQueryBackPressure semaphore = new SemaphoreProtectedReleaseQueryBackPressure(5);
        assertEquals(5, semaphore.availablePermits());
        for(int i=0;i<5;i++){
            semaphore.acquireBeforeQuery();
        }
        assertEquals(0, semaphore.availablePermits());

        for(int i=0;i<10;i++){
            semaphore.releaseAfterQuery();
        }
        while (semaphore.availablePermits() != 5){
            Thread.sleep(10);
        }

        for(int i=0;i<5;i++){
            semaphore.acquireBeforeQuery();
        }
        assertEquals(0, semaphore.availablePermits());
        semaphore.close();
        assertTrue(semaphore.hadWarningLogged());
    }

    @Test
    public void testExtraPermitsAreAddedWithSemaphoreQueryBackPressure() throws InterruptedException {
        SemaphoreQueryBackPressure semaphore = new SemaphoreQueryBackPressure(5);
        assertEquals(5, semaphore.availablePermits());
        for(int i=0;i<5;i++){
            semaphore.acquireBeforeQuery();
        }
        assertEquals(0, semaphore.availablePermits());

        for(int i=0;i<10;i++){
            semaphore.releaseAfterQuery();
        }
        while (semaphore.availablePermits() != 10){
            Thread.sleep(10);
        }

        for(int i=0;i<5;i++){
            semaphore.acquireBeforeQuery();
        }
        assertEquals(5, semaphore.availablePermits());
        semaphore.close();
    }

    @Test
    public void testSemaphoreQueryBackPressureInMultipleThreads() throws InterruptedException {
        int backPressureLimit = 5;
        testSemaphoreBasedQueryBackPressureInMultipleThreads(backPressureLimit,
            new SemaphoreQueryBackPressure(backPressureLimit));
    }

    @Test
    public void testSemaphoreProtectedReleaseQueryBackPressureInMultipleThreads() throws InterruptedException {
        int backPressureLimit = 5;
        testSemaphoreBasedQueryBackPressureInMultipleThreads(backPressureLimit,
            new SemaphoreProtectedReleaseQueryBackPressure(backPressureLimit));
    }

    private void testSemaphoreBasedQueryBackPressureInMultipleThreads(int backPressureLimit, QueryBackPressure backPressure) throws InterruptedException {
        int acquiresThreads = 20;
        ExecutorService acquireExecutorService = Executors.newFixedThreadPool(acquiresThreads);
        ExecutorService releaseExecutorService = Executors.newFixedThreadPool(acquiresThreads);

        assertEquals(backPressureLimit, availablePermits(backPressure));

        for(int i=0;i<backPressureLimit;i++){
            backPressure.acquireBeforeQuery();
        }
        assertEquals(0, availablePermits(backPressure));

        CountDownLatch acquiresCountDownLatch = new CountDownLatch(acquiresThreads);
        for(int i=0;i<acquiresThreads;i++){
            acquireExecutorService.execute(() -> {
                backPressure.acquireBeforeQuery();
                acquiresCountDownLatch.countDown();
            });
        }
        // Sleep for 2 seconds and check that no acquires are happened due to `backPressure` being exhausted
        Thread.sleep(2000);
        assertEquals(acquiresThreads, acquiresCountDownLatch.getCount());

        for(int i=0;i<acquiresThreads;i++){
            releaseExecutorService.execute(backPressure::releaseAfterQuery);
        }

        acquiresCountDownLatch.await();

        gracefulExecutorServiceShutdown(acquireExecutorService, Long.MAX_VALUE);
        gracefulExecutorServiceShutdown(releaseExecutorService, Long.MAX_VALUE);

        assertEquals(0, availablePermits(backPressure));

        for(int i=0;i<backPressureLimit;i++){
            backPressure.releaseAfterQuery();
        }

        backPressure.close();

        assertEquals(backPressureLimit, availablePermits(backPressure));
    }

    private int availablePermits(QueryBackPressure backPressure){
        if(backPressure instanceof SemaphoreProtectedReleaseQueryBackPressure){
            return ((SemaphoreProtectedReleaseQueryBackPressure) backPressure).availablePermits();
        }
        if(backPressure instanceof SemaphoreQueryBackPressure){
            return ((SemaphoreQueryBackPressure) backPressure).availablePermits();
        }
        throw new IllegalArgumentException("Cannot get available permits from "+backPressure.getClass().getName());
    }
}
