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
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MetricManagerTest {

    private final String name = "MetricManagerTest." + UUID.randomUUID();

    @AfterEach
    public void removeMetric() {
        MetricManager.INSTANCE.remove(name);
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
