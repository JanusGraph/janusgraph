package com.thinkaurelius.titan.diskstorage.locking.consistentkey;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.CacheBuilder;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStore;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import com.thinkaurelius.titan.diskstorage.util.KeyColumn;
import com.thinkaurelius.titan.diskstorage.util.UncaughtExceptionLogger;
import com.thinkaurelius.titan.diskstorage.util.UncaughtExceptionLogger.UELevel;

/**
 * Encapsulates an ExecutorService that creates and runs
 * {@link StandardLockCleanerRunnable} instances. Updates a timed-expiration Guava
 * strong cache keyed by rows to prevent the user from spamming
 * tens/hundreds/... of cleaner instances in a short time.
 */
public class StandardLockCleanerService implements LockCleanerService {

    private static final long KEEPALIVE_TIME = 5L;
    private static final TimeUnit KEEPALIVE_UNIT = TimeUnit.SECONDS;

    private static final long COOLDOWN_TIME = 30L;
    private static final TimeUnit COOLDOWN_UNIT = TimeUnit.SECONDS;
    private static final int COOLDOWN_CONCURRENCY_LEVEL = 4;

    private static final ThreadFactory THREAD_FACTORY =
            new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("LockCleaner-%d")
                .setUncaughtExceptionHandler(new UncaughtExceptionLogger(UELevel.INFO))
                .build();

    private final KeyColumnValueStore store;
    private final ExecutorService exec;
    private final ConcurrentMap<KeyColumn, Long> blocked;
    private final ConsistentKeyLockerSerializer serializer;

    private static final Logger log =
            LoggerFactory.getLogger(LockCleanerService.class);

    public StandardLockCleanerService(KeyColumnValueStore store, ConsistentKeyLockerSerializer serializer, ExecutorService exec, long cooldownTime, TimeUnit cooldownUnit) {
        this.store = store;
        this.serializer = serializer;
        this.exec = exec;
        blocked = CacheBuilder.newBuilder()
                .expireAfterWrite(cooldownTime, cooldownUnit)
                .concurrencyLevel(COOLDOWN_CONCURRENCY_LEVEL)
                .<KeyColumn, Long>build()
                .asMap();
    }

    public StandardLockCleanerService(KeyColumnValueStore store, ConsistentKeyLockerSerializer serializer) {
        this (store, serializer, getDefaultExecutor(), COOLDOWN_TIME, COOLDOWN_UNIT);
    }

    @Override
    public void clean(KeyColumn target, long cutoff, StoreTransaction tx) {
        Long b = blocked.putIfAbsent(target, cutoff);
        if (null == b) {
            log.info("Enqueuing expired lock cleaner task for target={}, tx={}, cutoff={}",
                    new Object[] { target, tx, cutoff });
            try {
                exec.submit(new StandardLockCleanerRunnable(store, target, tx, serializer, cutoff));
            } catch (RejectedExecutionException e) {
                log.debug("Failed to enqueue expired lock cleaner for target={}, tx={}, cutoff={}",
                        new Object[] { target, tx, cutoff, e });
            }
        } else {
            log.debug("Blocked redundant attempt to enqueue lock cleaner task for target={}, tx={}, cutoff={}",
                    new Object[] { target, tx, cutoff });
        }
    }

    private static ExecutorService getDefaultExecutor() {
        return new ThreadPoolExecutor(0, 1, KEEPALIVE_TIME, KEEPALIVE_UNIT, new LinkedBlockingQueue<Runnable>(), THREAD_FACTORY);
    }
}
