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

package org.janusgraph.diskstorage.util.time;

import com.google.common.base.Preconditions;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.TemporalUnit;

/**
 * A utility to measure time durations.
 * <p>
 * Differs from Guava Stopwatch in the following ways:
 *
 * <ul>
 * <li>encapsulates longs behind Instant/Duration</li>
 * <li>replacing the underlying Ticker with a TimestampProvider</li>
 * <li>can only be started and stopped once</li>
 * </ul>
 */
public class Timer {

    private final TimestampProvider times;
    private Instant start;
    private Instant stop;

    public Timer(final TimestampProvider times) {
        this.times = times;
    }

    public Timer start() {
        Preconditions.checkState(null == start, "Timer can only be started once");
        start = times.getTime();
        return this;
    }

    public Instant getStartTime() {
        Preconditions.checkNotNull(start, "Timer never started");
        return start;
    }

    public Timer stop() {
        Preconditions.checkNotNull(start, "Timer stopped before it was started");
        stop = times.getTime();
        return this;
    }

    public Duration elapsed() {
        if (null == start) {
            return Duration.ZERO;
        }
        final Instant stopTime = (null==stop? times.getTime() : stop);
        return Duration.between(start, stopTime);
    }

    public String toString() {
        TemporalUnit u = times.getUnit();
        if (start==null) return "Initialized";
        if (stop==null) return String.format("Started at %d %s",times.getTime(start),u);
        return String.format("%d %s", times.getTime(stop) - times.getTime(start), u);
    }
}
