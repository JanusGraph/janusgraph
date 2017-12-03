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

package org.janusgraph.diskstorage.keycolumnvalue.keyvalue;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.graphdb.query.BaseQuery;

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
        this(start,end, Predicates.alwaysTrue(),limit);
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
