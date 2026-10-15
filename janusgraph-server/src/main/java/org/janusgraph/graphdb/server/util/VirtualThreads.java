// Copyright 2026 JanusGraph Authors
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

package org.janusgraph.graphdb.server.util;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.concurrent.ThreadFactory;

/**
 * Creates virtual threads on Java 24 and later. JanusGraph is compiled for Java 11, which has no API for them, so the
 * factory is looked up at runtime.
 * <p>
 * Java 21 provides virtual threads, but before Java 24 (JEP 491) a virtual thread which waits inside a synchronized
 * method or block keeps the platform thread it runs on, its carrier. JanusGraph waits inside synchronized methods
 * while it commits a transaction ({@code StandardJanusGraphTx.commit()}) and while it waits for a block of IDs
 * ({@code StandardIDPool}), and the database cache ({@code cache.db-cache}) reads a missing entry from the storage
 * backend inside a synchronized block of {@code ConcurrentHashMap.compute}. On these Java versions, at most as many
 * of these waits proceed at once as there are carriers, by default one per processor, and while every carrier is
 * held that way, no other virtual thread runs.
 */
public final class VirtualThreads {

    /**
     * The first Java version on which a virtual thread which waits inside a synchronized method or block releases its
     * carrier.
     */
    public static final int MIN_JAVA_VERSION = 24;

    /**
     * The handles of {@code Thread.ofVirtual()}, {@code Thread.Builder.name(String, long)} and
     * {@code Thread.Builder.factory()}, looked up once; two threads may look them up at once and both keep their own.
     */
    private static volatile MethodHandle[] handles;

    private VirtualThreads() {
    }

    /**
     * @return whether the running JVM is Java 24 or later
     */
    public static boolean isSupported() {
        return Runtime.version().feature() >= MIN_JAVA_VERSION;
    }

    /**
     * Returns a factory of virtual threads, as {@code Thread.ofVirtual().name(prefix, start).factory()} does: the
     * threads are named after the prefix followed by a counter which starts at the given value.
     *
     * @throws UnsupportedOperationException if the running JVM is older than Java 24
     */
    public static ThreadFactory newThreadFactory(String prefix, long start) {
        if (!isSupported()) {
            throw new UnsupportedOperationException("JanusGraph uses virtual threads on Java " + MIN_JAVA_VERSION
                + " or later only, but this JVM runs Java " + Runtime.version());
        }
        MethodHandle[] handles = handles();
        try {
            Object builder = handles[0].invoke();
            builder = handles[1].invoke(builder, prefix, start);
            return (ThreadFactory) handles[2].invoke(builder);
        } catch (RuntimeException | Error e) {
            throw e;
        } catch (Throwable t) {
            throw new IllegalStateException("Could not create a factory of virtual threads on Java " + Runtime.version(), t);
        }
    }

    private static MethodHandle[] handles() {
        MethodHandle[] handles = VirtualThreads.handles;
        if (handles == null) {
            try {
                MethodHandles.Lookup lookup = MethodHandles.publicLookup();
                Class<?> builderType = Class.forName("java.lang.Thread$Builder");
                Class<?> ofVirtualType = Class.forName("java.lang.Thread$Builder$OfVirtual");
                handles = new MethodHandle[]{
                    lookup.findStatic(Thread.class, "ofVirtual", MethodType.methodType(ofVirtualType)),
                    lookup.findVirtual(builderType, "name", MethodType.methodType(builderType, String.class, long.class)),
                    lookup.findVirtual(builderType, "factory", MethodType.methodType(ThreadFactory.class))};
            } catch (ReflectiveOperationException e) {
                throw new IllegalStateException("Could not look up the virtual threads of Java " + Runtime.version(), e);
            }
            VirtualThreads.handles = handles;
        }
        return handles;
    }
}
