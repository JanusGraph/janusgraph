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

package org.janusgraph.util.stats;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.LockFreeExponentiallyDecayingReservoir;
import com.codahale.metrics.Reservoir;
import com.codahale.metrics.Timer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MetricManagerTest {

    private final String name = "MetricManagerTest." + UUID.randomUUID();

    @AfterEach
    public void removeMetric() {
        MetricManager.INSTANCE.remove(name);
        MetricManager.INSTANCE.removeSlf4jReporter();
    }

    //Reports to the default logger an hour after it starts, which no test lasts
    private static Runnable startSlf4jReporter() {
        return () -> MetricManager.INSTANCE.addSlf4jReporter(Duration.ofHours(1), null);
    }

    private static final MetricManager.GraphReporter SLF4J = MetricManager.GraphReporter.SLF4J;

    //The threads of the Slf4jReporters started since the given threads were alive
    private static List<Thread> slf4jReporterThreadsSince(Set<Thread> before) {
        return Thread.getAllStackTraces().keySet().stream()
            .filter(t -> !before.contains(t) && t.getName().startsWith("metrics-logger-reporter-"))
            .collect(Collectors.toList());
    }

    //The reporter's thread shows up once the reporter has started it, which its start doesn't wait for
    private static List<Thread> awaitSlf4jReporterThreadSince(Set<Thread> before) throws InterruptedException {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        List<Thread> threads = slf4jReporterThreadsSince(before);
        while (threads.isEmpty() && System.nanoTime() - deadline < 0) {
            Thread.sleep(10);
            threads = slf4jReporterThreadsSince(before);
        }
        return threads;
    }

    @Test
    public void aReporterGraphsStartedStopsOnceEveryClaimOnItIsReleased() throws InterruptedException {
        final Set<Thread> before = Thread.getAllStackTraces().keySet();
        final AtomicInteger starts = new AtomicInteger();
        final Runnable start = () -> {
            starts.incrementAndGet();
            startSlf4jReporter().run();
        };
        final Object first = MetricManager.INSTANCE.addGraphReporter(SLF4J, start);
        final Object second = MetricManager.INSTANCE.addGraphReporter(SLF4J, start);
        assertNotNull(first);
        assertNotNull(second);
        assertEquals(1, starts.get(), "the second graph started a reporter of its own");
        assertTrue(MetricManager.INSTANCE.isRunning(SLF4J));
        final List<Thread> reporterThreads = awaitSlf4jReporterThreadSince(before);
        assertEquals(1, reporterThreads.size(), "reporter threads: " + reporterThreads);

        MetricManager.INSTANCE.releaseGraphReporter(SLF4J, first);
        //Released twice, a claim counts once
        MetricManager.INSTANCE.releaseGraphReporter(SLF4J, first);
        assertTrue(MetricManager.INSTANCE.isRunning(SLF4J), "stopped while in use");
        MetricManager.INSTANCE.releaseGraphReporter(SLF4J, second);
        assertFalse(MetricManager.INSTANCE.isRunning(SLF4J));
        reporterThreads.get(0).join(TimeUnit.SECONDS.toMillis(10));
        assertFalse(reporterThreads.get(0).isAlive(), "the reporter was forgotten, but not stopped");

        //A graph which comes later starts it again
        final Object later = MetricManager.INSTANCE.addGraphReporter(SLF4J, start);
        assertEquals(2, starts.get());
        MetricManager.INSTANCE.releaseGraphReporters(Collections.singletonMap(SLF4J, later));
        assertFalse(MetricManager.INSTANCE.isRunning(SLF4J));
    }

    @Test
    public void graphsLeaveAReporterOtherCodeStartedRunning() {
        startSlf4jReporter().run();

        assertNull(MetricManager.INSTANCE.addGraphReporter(SLF4J, startSlf4jReporter()));
        MetricManager.INSTANCE.releaseGraphReporters(Collections.emptyMap());

        assertTrue(MetricManager.INSTANCE.isRunning(SLF4J));
    }

    @Test
    public void aClaimWhichOutlivesItsReporterLeavesTheNextOneRunning() {
        final Object stale = MetricManager.INSTANCE.addGraphReporter(SLF4J, startSlf4jReporter());
        //Other code removes the reporter, and a graph which opens then starts another
        MetricManager.INSTANCE.removeSlf4jReporter();
        final Object current = MetricManager.INSTANCE.addGraphReporter(SLF4J, startSlf4jReporter());

        MetricManager.INSTANCE.releaseGraphReporter(SLF4J, stale);
        assertTrue(MetricManager.INSTANCE.isRunning(SLF4J), "a stale claim stopped the reporter of another graph");

        MetricManager.INSTANCE.releaseGraphReporter(SLF4J, current);
        assertFalse(MetricManager.INSTANCE.isRunning(SLF4J));
    }

    @Test
    public void removingAReporterGraphsUseEndsTheirClaimsOnIt() {
        final Object claim = MetricManager.INSTANCE.addGraphReporter(SLF4J, startSlf4jReporter());
        MetricManager.INSTANCE.removeSlf4jReporter();
        //Started by other code now, which the graph's release must leave running
        startSlf4jReporter().run();

        MetricManager.INSTANCE.releaseGraphReporter(SLF4J, claim);

        assertTrue(MetricManager.INSTANCE.isRunning(SLF4J));
    }

    @Test
    public void aSecondGraphiteReporterIsNotStartedBesideTheFirst() throws Exception {
        //A reporter connects only when it reports, an hour after it starts and once more when it stops
        MetricManager.INSTANCE.addGraphiteReporter("127.0.0.1", 9, null, Duration.ofHours(1));
        try {
            final Object first = read(MetricManager.INSTANCE, MetricManager.class, "graphiteReporter");
            MetricManager.INSTANCE.addGraphiteReporter("127.0.0.1", 9, null, Duration.ofHours(1));
            assertSame(first, read(MetricManager.INSTANCE, MetricManager.class, "graphiteReporter"));
        } finally {
            MetricManager.INSTANCE.removeGraphiteReporter();
        }
    }

    @Test
    public void timerSamplesIntoALockFreeReservoir() throws Exception {
        Timer timer = MetricManager.INSTANCE.getTimer(name);
        timer.update(5, TimeUnit.MILLISECONDS);
        timer.update(7, TimeUnit.MILLISECONDS);

        assertTrue(reservoirOf(histogramOf(timer)) instanceof LockFreeExponentiallyDecayingReservoir);
        assertEquals(2, timer.getCount());
        assertEquals(TimeUnit.MILLISECONDS.toNanos(7), timer.getSnapshot().getMax());
    }

    @Test
    public void histogramSamplesIntoALockFreeReservoir() throws Exception {
        Histogram histogram = MetricManager.INSTANCE.getHistogram(name);
        histogram.update(3);

        assertTrue(reservoirOf(histogram) instanceof LockFreeExponentiallyDecayingReservoir);
        assertEquals(1, histogram.getCount());
        assertEquals(3, histogram.getSnapshot().getMax());
    }

    @Test
    public void repeatedLookupsReturnTheRegisteredTimer() {
        Timer first = MetricManager.INSTANCE.getTimer(name);
        assertSame(first, MetricManager.INSTANCE.getTimer(name));
        assertSame(first, MetricManager.INSTANCE.getTimer("MetricManagerTest", name.substring("MetricManagerTest.".length())));
    }

    private static Histogram histogramOf(Timer timer) throws Exception {
        return (Histogram) read(timer, Timer.class, "histogram");
    }

    private static Reservoir reservoirOf(Histogram histogram) throws Exception {
        return (Reservoir) read(histogram, Histogram.class, "reservoir");
    }

    private static Object read(Object target, Class<?> type, String field) throws Exception {
        Field f = type.getDeclaredField(field);
        f.setAccessible(true);
        return f.get(target);
    }
}
