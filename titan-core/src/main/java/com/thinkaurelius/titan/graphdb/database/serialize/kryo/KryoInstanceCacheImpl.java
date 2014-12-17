package com.thinkaurelius.titan.graphdb.database.serialize.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;

public enum KryoInstanceCacheImpl {

    /**
     * Uses {@link java.lang.ThreadLocal}.  This implementation's {@link java.io.Closeable#close()}
     * method is not guaranteed to clean up all threadlocal state created by the implementation.
     */
    THREAD_LOCAL {
        @Override
        public KryoInstanceCache createManager(Function<Kryo, ?> configurer) {
            return new ThreadLocalKryos(configurer);
        }
    },

    /**
     * Uses a {@code ConcurrentHashMap} keyed by {@code Thread.currentThread()}.  This implementation's
     * {@link java.io.Closeable#close()} clears the {@code ConcurrentHashMap}.
     */
    CONCURRENT_HASH_MAP {
        @Override
        public KryoInstanceCache createManager(Function<Kryo, ?> configurer) {
            return new CHMKryos(configurer);
        }
    };

    /**
     * Construct a {@link com.thinkaurelius.titan.graphdb.database.serialize.kryo.KryoInstanceCache}
     * which invokes the supplied {@code configurer} function exactly once every Kryo it instantiates,
     * just after constructing the Kryo instance but before returning it to the caller.
     *
     * @param configurer an arbitrary function for customizing Kryo instances
     *                   constructed by the returned KryoInstanceCache
     * @return the KryoInstanceCache
     */
    public abstract KryoInstanceCache createManager(Function<Kryo, ?> configurer);
}

class ThreadLocalKryos extends ThreadLocal<Kryo> implements KryoInstanceCache {

    private final Function<Kryo, ?> configurer;
    private final String selfID;

    private static final Logger log =
            LoggerFactory.getLogger(ThreadLocalKryos.class);

    public ThreadLocalKryos(Function<Kryo, ?> configurer) {
        this.configurer = configurer;
        this.selfID = String.format("0x%-8s", Integer.toHexString(hashCode()));
        log.debug("[{}] Finishing construction", selfID);
    }

    @Override
    public Kryo initialValue() {
        Kryo k = new Kryo();
        configurer.apply(k);
        log.debug("[{}] Returning thread-local Kryo {}", selfID, k);
        return k;
    }

    @Override
    public void close() {
        // This only cleans up the ThreadLocal associated with the calling thread.
        // If this instance was used on other threads besides the one that calls
        // close, then those other threads may retain unremoved ThreadLocal data.
        remove();
        log.debug("[{}] Removed thread-local", selfID);
    }
}

class CHMKryos implements KryoInstanceCache {

    private final Function<Kryo, ?> configurer;
    private final ConcurrentHashMap<Thread, Kryo> kryos;
    private final String selfID;
    private static final Logger log = LoggerFactory.getLogger(CHMKryos.class);

    public CHMKryos(Function<Kryo, ?> configurer) {
        this.configurer = configurer;
        this.kryos = new ConcurrentHashMap<Thread, Kryo>();
        this.selfID = String.format("0x%-8s", Integer.toHexString(hashCode()));
        log.debug("[{}] Finishing construction", selfID);
    }

    @Override
    public Kryo get() {
        Thread currentThread = Thread.currentThread();
        Kryo k = kryos.get(currentThread);
        if (null == k) {
            k = new Kryo();
            configurer.apply(k);
            Kryo shouldBeNull = kryos.putIfAbsent(currentThread, k);
            Preconditions.checkState(null == shouldBeNull);
            // Logging the thread is potentially redundant with the Log4j/Logback format,
            // but it's still useful in case the threadid is not part of the general log format
            log.debug("[{}] Returning new Kryo instance {} for thread {}", selfID, k, currentThread);
        } else {
            log.debug("[{}] Returning cached Kryo instance {} for thread {}", selfID, k, currentThread);
        }
        return k;
    }

    @Override
    public void close() {
        final int sizeEstimate = kryos.size();
        kryos.clear();
        log.debug("[{}] Cleared approximately {} cached Kryo instance(s)", selfID, sizeEstimate);
    }
}
