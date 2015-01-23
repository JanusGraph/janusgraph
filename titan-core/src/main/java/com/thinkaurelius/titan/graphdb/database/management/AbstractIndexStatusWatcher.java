package com.thinkaurelius.titan.graphdb.database.management;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.attribute.Duration;
import com.thinkaurelius.titan.core.schema.SchemaStatus;
import com.thinkaurelius.titan.diskstorage.util.time.StandardDuration;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

public abstract class AbstractIndexStatusWatcher<R, S extends AbstractIndexStatusWatcher<R,S>> implements Callable<R> {

    protected TitanGraph g;
    protected SchemaStatus status;
    protected Duration timeout;
    protected Duration poll;

    public AbstractIndexStatusWatcher(TitanGraph g) {
        this.g = g;
        this.status = SchemaStatus.REGISTERED;
        this.timeout = new StandardDuration(60L, TimeUnit.SECONDS);
        this.poll = new StandardDuration(500L, TimeUnit.MILLISECONDS);
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
    public S timeout(long timeout, TimeUnit timeoutUnit) {
        if (0 > timeout) {
            this.timeout = null;
        } else {
            this.timeout = new StandardDuration(timeout, timeoutUnit);
        }
        return self();
    }

    /**
     * Set the index information polling interval.  {@link #call()} waits
     * at least this long between repeated attempts to read schema information
     * and determine whether the index has reached its target state.
     */
    public S pollInterval(long poll, TimeUnit pollUnit) {
        Preconditions.checkArgument(0 <= poll);
        this.poll = new StandardDuration(poll, pollUnit);
        return self();
    }

}
