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



import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

/**
 * System time interface that abstracts time units, resolution, and measurements of time.
 */
public interface TimestampProvider {

    /**
     * Returns the current time based on this timestamp provider
     * as a {@link Instant}.
     *
     * @return
     */
    Instant getTime();

    /**
     * Returns the given time as a {@link Instant} based off of this timestamp providers units
     * @param sinceEpoch
     * @return
     */
    Instant getTime(long sinceEpoch);

    /**
     * Return the units of {@link #getTime()}. This method's return value must
     * be constant over at least the life of the object implementing this
     * interface.
     *
     * @return this instance's time unit
     */
    ChronoUnit getUnit();

    /**
     * Block until the current time as returned by {@link #getTime()} is greater
     * than the given timepoint.
     *
     * @param futureTime The time to sleep past
     *
     * @return the current time in the same units as the {@code unit} argument
     * @throws InterruptedException
     *             if externally interrupted
     */
    Instant sleepPast(Instant futureTime) throws InterruptedException;

    /**
     * Sleep for the given duration of time.
     *
     * @param duration
     * @throws InterruptedException
     */
    void sleepFor(Duration duration) throws InterruptedException;

    /**
     * Returns a {@link Timer} based on this timestamp provider
     *
     * @return
     */
    Timer getTimer();


    /**
     * Returns the scalar value for this instant given the configured time unit
     * @param timestamp
     * @return
     */
    long getTime(Instant timestamp);
}
