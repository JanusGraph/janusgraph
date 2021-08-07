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

package org.janusgraph.diskstorage.configuration;

import com.codahale.metrics.InstrumentedExecutorService;
import com.codahale.metrics.InstrumentedThreadFactory;
import com.codahale.metrics.MetricRegistry;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class ExecutorServiceInstrumentationTest {

    @Test
    public void shouldCreateInstrumentedExecutorService() throws InterruptedException {
        MetricRegistry registry = new MetricRegistry();
        InstrumentedExecutorService executorService = ExecutorServiceInstrumentation.instrument(
            "org.janusgraph",
            "test",
            Executors.newFixedThreadPool(1),
            registry
        );
        try {
            Lock lock = new ReentrantLock();
            lock.lock();
            try {
                // Spawn two threads: (1) will be running waiting for lock to be available ; (2) will wait in queue
                executorService.execute(lock::lock);
                executorService.execute(() -> {});
                // Wait for the thread to start
                Thread.sleep(100);

                assertEquals(1, registry.getGauges().get("org.janusgraph.threadpools.test.executorService.queueSize").getValue());
                assertEquals(1L, registry.getCounters().get("org.janusgraph.threadpools.test.executorService.running").getCount());
                assertEquals(0L, registry.getMeters().get("org.janusgraph.threadpools.test.executorService.completed").getCount());
                assertEquals(2L, registry.getMeters().get("org.janusgraph.threadpools.test.executorService.submitted").getCount());
                assertNotNull(registry.getTimers().get("org.janusgraph.threadpools.test.executorService.duration"));
            } finally {
                lock.unlock();
            }
        } finally {
            executorService.shutdown();
        }
        // Wait for the thread to stop
        Thread.sleep(100);
        assertEquals(2L, registry.getMeters().get("org.janusgraph.threadpools.test.executorService.completed").getCount());
    }

    @Test
    public void shouldCreateInstrumentedThreadFactory() throws InterruptedException {
        MetricRegistry registry = new MetricRegistry();
        InstrumentedThreadFactory threadFactory = ExecutorServiceInstrumentation.instrument(
            "org.janusgraph",
            "test",
            new ThreadFactoryBuilder().build(),
            registry
        );
        ExecutorService executorService = Executors.newSingleThreadExecutor(threadFactory);
        try {
            Lock lock = new ReentrantLock();
            lock.lock();
            try {
                // Spawn a thread that will wait for the lock to become available
                executorService.submit(lock::lock);

                // Wait for the thread to start
                Thread.sleep(100);

                assertEquals(1L, registry.getMeters().get("org.janusgraph.threadpools.test.threadFactory.created").getCount());
                assertEquals(1L, registry.getCounters().get("org.janusgraph.threadpools.test.threadFactory.running").getCount());
                assertEquals(0L, registry.getMeters().get("org.janusgraph.threadpools.test.threadFactory.terminated").getCount());
            } finally {
                lock.unlock();
            }
        } finally {
            executorService.shutdown();
        }
        // Wait for the thread to stop
        Thread.sleep(100);
        assertEquals(1L, registry.getMeters().get("org.janusgraph.threadpools.test.threadFactory.terminated").getCount());
    }
}
