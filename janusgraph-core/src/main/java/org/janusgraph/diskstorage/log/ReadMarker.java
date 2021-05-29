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

package org.janusgraph.diskstorage.log;

import com.google.common.base.Preconditions;
import org.janusgraph.diskstorage.util.time.TimestampProvider;

import java.time.Instant;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class ReadMarker {

    private final String identifier;
    private Instant startTime;



    private ReadMarker(String identifier, Instant startTime) {
        this.identifier = identifier;
        this.startTime = startTime;
    }

    /**
     * Whether this read marker has a configured identifier
     * @return
     */
    public boolean hasIdentifier() {
        return identifier!=null;
    }

    /**
     * Returns the configured identifier of this marker or throws an exception if none exists.
     * @return
     */
    public String getIdentifier() {
        Preconditions.checkArgument(identifier!=null,"ReadMarker does not have a configured identifier");
        return identifier;
    }

    public boolean hasStartTime() {
        return startTime!=null;
    }

    /**
     * Returns the start time of this marker if such has been defined or the current time if not
     * @return
     */
    public synchronized Instant getStartTime(TimestampProvider times) {
        if (startTime==null) {
            startTime = times.getTime();
        }
        return startTime;
    }

    /**
     *
     * @param newMarker
     * @return
     */
    public boolean isCompatible(ReadMarker newMarker) {
        if (newMarker.hasIdentifier()) {
            return hasIdentifier() && identifier.equals(newMarker.identifier);
        }
        return !newMarker.hasStartTime();
    }

    /**
     * Starts reading the log such that it will start with the first entry written after now.
     *
     * @return
     */
    public static ReadMarker fromNow() {
        return new ReadMarker(null, null);
    }

    /**
     * Starts reading the log from the given timestamp onward. The specified timestamp is included.
     * @param timestamp
     * @return
     */
    public static ReadMarker fromTime(Instant timestamp) {
        return new ReadMarker(null, timestamp);
    }

    /**
     * Starts reading the log from the last recorded point in the log for the given id.
     * If the log has a record of such an id, it will use it as the starting point.
     * If not, it will start from the given timestamp and set it as the first read record for the given id.
     * <p>
     * Identified read markers of this kind are useful to continuously read from the log. In the case of failure,
     * the last read record can be recovered for the id and log reading can be resumed from there. Note, that some
     * records might be read twice in that event depending on the guarantees made by a particular implementation.
     *
     * @param id
     * @param timestamp
     * @return
     */
    public static ReadMarker fromIdentifierOrTime(String id, Instant timestamp) {
        return new ReadMarker(id, timestamp);
    }

    /**
     * Like {@link #fromIdentifierOrTime(String id, Instant timestamp)} but uses the current time point
     * as the starting timestamp if the log has no record of the id.
     *
     * @param id
     * @return
     */
    public static ReadMarker fromIdentifierOrNow(String id) {
        return new ReadMarker(id, Instant.EPOCH);
    }

}
