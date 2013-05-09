package com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;

/**
 * A {@link KeySelector} that returns keys up to a given limit.
 *
 * @author Matthias Br&ouml;cheler (me@matthiasb.com);
 */
public class LimitedSelector implements KeySelector {

    private final int limit;
    private int count;

    public LimitedSelector(int limit) {
        Preconditions.checkArgument(limit > 0, "The count limit needs to be positive. Given: " + limit);
        this.limit = limit;
        count = 0;
    }

    public static final LimitedSelector of(int limit) {
        return new LimitedSelector(limit);
    }

    @Override
    public boolean include(StaticBuffer key) {
        count++;
        return true;
    }

    @Override
    public boolean reachedLimit() {
        if (count >= limit) return true;
        else return false;
    }

}
