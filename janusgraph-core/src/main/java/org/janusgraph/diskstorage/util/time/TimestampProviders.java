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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.TimeUnit;

/**
 * Implementations of {@link TimestampProvider} for different resolutions of time:
 * <ul>
 *     <li>NANO: nano-second time resolution based on System.nanoTime using a base-time established
 *     by System.currentTimeMillis(). The exact resolution depends on the particular JVM and host machine.</li>
 *     <li>MICRO: micro-second time which is actually at milli-second resolution.</li>
 *     <li>MILLI: milli-second time resolution</li>
 * </ul>
 */
public enum TimestampProviders implements TimestampProvider {
    NANO {

        /**
         * This returns the approximate number of nanoseconds
         * elapsed since the UNIX Epoch.  The least significant
         * bit is overridden to 1 or 0 depending on whether
         * setLSB is true or false (respectively).
         * <p/>
         * This timestamp rolls over about every 2^63 ns, or
         * just over 292 years.  The first rollover starting
         * from the UNIX Epoch would be sometime in 2262.
         *
         * @return a timestamp as described above
         */
        @Override
        public Instant getTime() {
            return Instant.now();
        }

        @Override
        public Instant getTime(long sinceEpoch) {
            return Instant.ofEpochSecond(0, sinceEpoch);
        }

        @Override
        public ChronoUnit getUnit() {
            return ChronoUnit.NANOS;
        }

        @Override
        public long getTime(Instant timestamp) {
            return timestamp.getEpochSecond() * 1000000000L + timestamp.getNano();
        }
    },

    MICRO {
        @Override
        public Instant getTime() {
            return Instant.now().truncatedTo(getUnit());
        }

        @Override
        public Instant getTime(long sinceEpoch) {
            return Instant.ofEpochSecond(0, (sinceEpoch * 1000L));
        }

        @Override
        public ChronoUnit getUnit() {
            return ChronoUnit.MICROS;
        }

        @Override
        public long getTime(Instant timestamp) {
            return timestamp.getEpochSecond() * 1000000L + timestamp.getNano()/1000;

        }
    },

    MILLI {
        @Override
        public Instant getTime() {
            return Instant.now().truncatedTo(getUnit());
        }

        @Override
        public Instant getTime(long sinceEpoch) {
            return Instant.ofEpochMilli(sinceEpoch);
        }

        @Override
        public ChronoUnit getUnit() {
            return ChronoUnit.MILLIS;
        }

        @Override
        public long getTime(Instant timestamp) {
            return timestamp.getEpochSecond() * 1000 + timestamp.getNano() / 1000000;
        }
    };

    private static final Logger log =
            LoggerFactory.getLogger(TimestampProviders.class);

    @Override
    public Instant sleepPast(Instant futureTime) throws InterruptedException {

        Instant now;

        ChronoUnit unit = getUnit();

        /*
         * Distributed storage managers that rely on timestamps play with the
         * least significant bit in timestamp longs, turning it on or off to
         * ensure that deletions are logically ordered before additions within a
         * single batch mutation. This is not a problem at microsecond
         * resolution because we pretend to have microsecond resolution by
         * multiplying currentTimeMillis by 1000, so the LSB can vary freely.
         * It's also not a problem with nanosecond resolution because the
         * resolution is just too fine, relative to how long a mutation takes,
         * for it to matter in practice. But it can lead to corruption at
         * millisecond resolution (and does, in testing).
         */
        if (unit.equals(ChronoUnit.MILLIS))
            futureTime = futureTime.plusMillis(1L);

        while ((now = getTime()).compareTo(futureTime) <= 0) {

            long delta = getTime(futureTime) - getTime(now);

            if (0L == delta)
                delta = 1L;

            if (log.isTraceEnabled()) {
                log.trace("Sleeping: now={} targettime={} delta={} {}", now, futureTime, delta, unit);
            }

            Temporals.timeUnit(unit).sleep(delta);
        }

        return now;
    }

    @Override
    public void sleepFor(Duration duration) throws InterruptedException {
        if (duration.isZero()) return;

        TimeUnit.NANOSECONDS.sleep(duration.toNanos());
    }

    @Override
    public Timer getTimer() {
        return new Timer(this);
    }

    @Override
    public String toString() {
        return name();
    }


}
