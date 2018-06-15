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

package org.janusgraph.core.log;

import java.time.Instant;

/**
 * Builder for assembling a processor that processes a particular transaction log. A processor can be composed of one or multiple
 * {@link ChangeProcessor}s which are executed independently.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface LogProcessorBuilder {

    /**
     * Returns the identifier of the transaction log to be processed by this processor.
     *
     * @return
     */
    String getLogIdentifier();

    /**
     * Sets the identifier of this processor. This String should uniquely identify a log processing instance and will be used to record
     * up to which position in the log the log processor has advanced. In case of instance failure or instance restart,
     * the log processor can then pick up where it left of.
     * <p>
     * This is an optional argument if recording the processing state is desired.
     *
     * @param name
     * @return
     */
    LogProcessorBuilder setProcessorIdentifier(String name);

    /**
     * Sets the time at which this log processor should start processing transaction log entries
     *
     * @param startTime
     * @return
     */
    LogProcessorBuilder setStartTime(Instant startTime);

    /**
     * Indicates that the transaction log processor should process newly added events.
     *
     * @return
     */
    LogProcessorBuilder setStartTimeNow();

    /**
     * Adds a {@link ChangeProcessor} to this transaction log processor. These are executed independently.
     * @param processor
     * @return
     */
    LogProcessorBuilder addProcessor(ChangeProcessor processor);

    /**
     * Sets how often this log processor should attempt to retry executing a contained {@link ChangeProcessor} in case of failure.
     * @param attempts
     * @return
     */
    LogProcessorBuilder setRetryAttempts(int attempts);

    /**
     * Builds this transaction log processor and starts processing the log.
     */
    void build();

}
