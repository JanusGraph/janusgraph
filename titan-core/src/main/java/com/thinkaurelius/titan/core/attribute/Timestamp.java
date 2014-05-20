package com.thinkaurelius.titan.core.attribute;

import java.util.concurrent.TimeUnit;

/**
 * A point in time as measured in elapsed time since UNIX Epoch time
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface Timestamp extends Comparable<Timestamp> {

    /**
     * Returns the length of time since UNIX epoch in the given {@link java.util.concurrent.TimeUnit}.
     *
     * @param unit
     * @return
     */
    public long sinceEpoch(TimeUnit unit);

    /**
     * Returns the native unit used by this Timestamp. The actual time is specified in this unit of time.
     * </p>
     * @return
     */
    public TimeUnit getNativeUnit();

}
