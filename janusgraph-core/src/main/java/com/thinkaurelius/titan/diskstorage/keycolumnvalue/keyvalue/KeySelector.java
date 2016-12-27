package com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;

/**
 * A {@link KeySelector} utility that can be generated out of a given {@link KVQuery}
 *
 * @author Matthias Br&ouml;cheler (me@matthiasb.com);
 */
public class KeySelector {

    private final Predicate<StaticBuffer> keyFilter;
    private final int limit;
    private int count;

    public KeySelector(Predicate<StaticBuffer> keyFilter, int limit) {
        Preconditions.checkArgument(limit > 0, "The count limit needs to be positive. Given: " + limit);
        Preconditions.checkArgument(keyFilter!=null);
        this.keyFilter = keyFilter;
        this.limit = limit;
        count = 0;
    }

    public static final KeySelector of(int limit) {
        return new KeySelector(Predicates.<StaticBuffer>alwaysTrue(), limit);
    }

    public boolean include(StaticBuffer key) {
        if (keyFilter.apply(key)) {
            count++;
            return true;
        } else return false;
    }

    public boolean reachedLimit() {
        if (count >= limit) return true;
        else return false;
    }

}
