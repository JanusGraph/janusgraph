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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledForJreRange;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class VirtualThreadsTest {

    /**
     * Uses {@code Thread.isVirtual()} of Java 21 and later through reflection; false before Java 21.
     */
    public static boolean isVirtual(Thread thread) throws ReflectiveOperationException {
        if (Runtime.version().feature() < 21) {
            return false;
        }
        return (boolean) Thread.class.getMethod("isVirtual").invoke(thread);
    }

    @Test
    @EnabledForJreRange(maxVersion = VirtualThreads.MIN_JAVA_VERSION - 1)
    public void testFactoryRefusedBeforeJava24() {
        assertFalse(VirtualThreads.isSupported());
        UnsupportedOperationException e = assertThrows(UnsupportedOperationException.class,
            () -> VirtualThreads.newThreadFactory("test-", 1));
        assertTrue(e.getMessage().startsWith("JanusGraph uses virtual threads on Java 24 or later only, but this JVM "
            + "runs Java "), e.getMessage());
    }

    @Test
    @EnabledForJreRange(minVersion = VirtualThreads.MIN_JAVA_VERSION)
    public void testFactoryCreatesNamedVirtualThreads() throws Exception {
        assertTrue(VirtualThreads.isSupported());
        ThreadFactory factory = VirtualThreads.newThreadFactory("test-", 5);
        AtomicReference<Thread> ran = new AtomicReference<>();
        Thread first = factory.newThread(() -> ran.set(Thread.currentThread()));
        Thread second = factory.newThread(() -> {});

        assertTrue(isVirtual(first));
        assertTrue(isVirtual(second));
        assertEquals("test-5", first.getName());
        assertEquals("test-6", second.getName());

        first.start();
        first.join();
        assertSame(first, ran.get());
    }
}
