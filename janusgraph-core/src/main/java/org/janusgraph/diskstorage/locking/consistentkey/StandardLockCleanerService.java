// Copyright 2017 JanusGraph Authors
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

package org.janusgraph.diskstorage.locking.consistentkey;

import com.google.common.cache.CacheBuilder;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStore;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.util.KeyColumn;
import org.janusgraph.diskstorage.util.UncaughtExceptionLogger;
import org.janusgraph.diskstorage.util.UncaughtExceptionLogger.UELevel;
import org.janusgraph.diskstorage.util.time.TimestampProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Encapsulates an ExecutorService that creates and runs
 * {@link StandardLockCleanerRunnable} instances. Updates a timed-expiration Guava
 * strong cache keyed by rows to prevent the user from spamming
 * tens/hundreds/... of cleaner instances in a short time.
 */
public class StandardLockCleanerService implements LockCleanerService {

    private static final long KEEPALIVE_TIME = 5L;
    private static final TimeUnit KEEPALIVE_UNIT = TimeUnit.SECONDS;

    private static final Duration COOLDOWN_TIME = Duration.ofSeconds(30);

    private static final int COOLDOWN_CONCURRENCY_LEVEL = 4;

    private static final ThreadFactory THREAD_FACTORY =
            new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("LockCleaner-%d")
                .setUncaughtExceptionHandler(new UncaughtExceptionLogger(UELevel.INFO))
                .build();

    private final KeyColumnValueStore store;
    private final ExecutorService exec;
    private final TimestampProvider times;
    private final ConcurrentMap<KeyColumn, Instant> blocked;
    private final ConsistentKeyLockerSerializer serializer;

    private static final Logger log =
            LoggerFactory.getLogger(LockCleanerService.class);

    public StandardLockCleanerService(KeyColumnValueStore store, ConsistentKeyLockerSerializer serializer,
                                      ExecutorService exec, Duration cooldown, TimestampProvider times) {
        this.store = store;
        this.serializer = serializer;
        this.exec = exec;
        this.times = times;
        blocked = CacheBuilder.newBuilder()
                .expireAfterWrite(cooldown.toNanos(), TimeUnit.NANOSECONDS)
                .concurrencyLevel(COOLDOWN_CONCURRENCY_LEVEL)
                .<KeyColumn, Instant>build()
                .asMap();
    }

    public StandardLockCleanerService(KeyColumnValueStore store, ConsistentKeyLockerSerializer serializer,
                                      TimestampProvider times) {
        this (store, serializer, getDefaultExecutor(), COOLDOWN_TIME, times);
    }

    @Override
    public void clean(KeyColumn target, Instant cutoff, StoreTransaction tx) {
        Instant b = blocked.putIfAbsent(target, cutoff);
        if (null == b) {
            log.info("Enqueuing expired lock cleaner task for target={}, tx={}, cutoff={}", target, tx, cutoff);
            try {
                exec.submit(new StandardLockCleanerRunnable(store, target, tx, serializer, cutoff, times));
            } catch (RejectedExecutionException e) {
                log.debug("Failed to enqueue expired lock cleaner for target={}, tx={}, cutoff={}",
                    target, tx, cutoff, e);
            }
        } else {
            log.debug("Blocked redundant attempt to enqueue lock cleaner task for target={}, tx={}, cutoff={}",
                target, tx, cutoff);
        }
    }

    private static ExecutorService getDefaultExecutor() {
        return new ThreadPoolExecutor(0, 1, KEEPALIVE_TIME, KEEPALIVE_UNIT,
                new LinkedBlockingQueue<>(), THREAD_FACTORY);
    }
}
