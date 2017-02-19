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

package org.janusgraph.graphdb.database.management;

import com.google.common.base.Preconditions;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.schema.SchemaStatus;


import java.time.Duration;
import java.time.temporal.TemporalUnit;
import java.util.concurrent.Callable;

public abstract class AbstractIndexStatusWatcher<R, S extends AbstractIndexStatusWatcher<R,S>> implements Callable<R> {

    protected JanusGraph g;
    protected SchemaStatus status;
    protected Duration timeout;
    protected Duration poll;

    public AbstractIndexStatusWatcher(JanusGraph g) {
        this.g = g;
        this.status = SchemaStatus.REGISTERED;
        this.timeout = Duration.ofSeconds(60L);
        this.poll = Duration.ofMillis(500L);
    }

    protected abstract S self();

    /**
     * Set the target index status.  {@link #call()} will repeatedly
     * poll the graph passed into this instance during construction to
     * see whether the index (also passed in during construction) has
     * the supplied status.
     *
     * @param status
     * @return
     */
    public S status(SchemaStatus status) {
        this.status = status;
        return self();
    }

    /**
     * Set the maximum amount of wallclock time that {@link #call()} will
     * wait for the index to reach the target status.  If the index does
     * not reach the target state in this interval, then {@link #call()}
     * will return a report value indicating failure.
     * <p>
     * A negative {@code timeout} is interpreted to mean "wait forever"
     * (no timeout).  In this case, the {@code timeoutUnit} is ignored.
     *
     * @param timeout the time duration scalar
     * @param timeoutUnit the time unit
     * @return this builder
     */
    public S timeout(long timeout, TemporalUnit timeoutUnit) {
        if (0 > timeout) {
            this.timeout = null;
        } else {
            this.timeout = Duration.of(timeout, timeoutUnit);
        }
        return self();
    }

    /**
     * Set the index information polling interval.  {@link #call()} waits
     * at least this long between repeated attempts to read schema information
     * and determine whether the index has reached its target state.
     */
    public S pollInterval(long poll, TemporalUnit pollUnit) {
        Preconditions.checkArgument(0 <= poll);
        this.poll = Duration.of(poll, pollUnit);
        return self();
    }

}
