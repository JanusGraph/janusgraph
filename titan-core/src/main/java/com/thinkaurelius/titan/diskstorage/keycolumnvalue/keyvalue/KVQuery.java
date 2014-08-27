package com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.graphdb.query.BaseQuery;

/**
 * A query against a {@link OrderedKeyValueStore}. Retrieves all the results that lie between start (inclusive) and
 * end (exclusive) which satisfy the filter. Returns up to the specified limit number of key-value pairs {@link KeyValueEntry}.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class KVQuery extends BaseQuery {

    private final StaticBuffer start;
    private final StaticBuffer end;
    private final Predicate<StaticBuffer> keyFilter;

    public KVQuery(StaticBuffer start, StaticBuffer end) {
        this(start,end,BaseQuery.NO_LIMIT);
    }

    public KVQuery(StaticBuffer start, StaticBuffer end, int limit) {
        this(start,end, Predicates.<StaticBuffer>alwaysTrue(),limit);
    }

    public KVQuery(StaticBuffer start, StaticBuffer end, Predicate<StaticBuffer> keyFilter, int limit) {
        super(limit);
        this.start = start;
        this.end = end;
        this.keyFilter = keyFilter;
    }

    public StaticBuffer getStart() {
        return start;
    }

    public StaticBuffer getEnd() {
        return end;
    }

    public KeySelector getKeySelector() {
        return new KeySelector(keyFilter,getLimit());
    }


}
