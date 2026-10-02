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

package org.janusgraph.graphdb.query.graph;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.janusgraph.diskstorage.BackendTransaction;
import org.janusgraph.diskstorage.EntryList;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.keycolumnvalue.KeySliceQuery;
import org.janusgraph.diskstorage.keycolumnvalue.SliceQuery;
import org.janusgraph.diskstorage.util.EntryArrayList;
import org.janusgraph.graphdb.query.BackendQuery;
import org.janusgraph.graphdb.query.BaseQuery;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class MultiKeySliceQuery extends BaseQuery implements BackendQuery<MultiKeySliceQuery>  {

    /**
     * The most keys one call to the storage backend reads. A retry, a timeout and an interruption then concern a bounded
     * part of a long {@code within()}, and so does what the backend holds in memory for the call.
     */
    static final int MAX_KEYS_PER_READ = 1_000;

    private final List<KeySliceQuery> queries;
    private final boolean atMostOneEntryPerKey;
    private final boolean sharesOneSlice;

    public MultiKeySliceQuery(List<KeySliceQuery> queries) {
        this(queries, false);
    }

    /**
     * @param queries              the query of each key. The query keeps its own copy of the list, so that a later
     *                             change to the list changes neither the keys it reads nor how it reads them
     * @param atMostOneEntryPerKey whether each key holds at most one entry, as each key of a unique composite index
     *                             does. It lets the query read as many keys at once as its limit can still take
     */
    public MultiKeySliceQuery(List<KeySliceQuery> queries, boolean atMostOneEntryPerKey) {
        Preconditions.checkArgument(queries!=null && !queries.isEmpty());
        this.queries = ImmutableList.copyOf(queries);
        this.atMostOneEntryPerKey = atMostOneEntryPerKey;
        this.sharesOneSlice = sharesOneSlice(this.queries);
    }

    //The same keys, read the same way: shares the copy of their queries
    private MultiKeySliceQuery(MultiKeySliceQuery query) {
        this.queries = query.queries;
        this.atMostOneEntryPerKey = query.atMostOneEntryPerKey;
        this.sharesOneSlice = query.sharesOneSlice;
    }

    @Override
    public MultiKeySliceQuery updateLimit(int newLimit) {
        MultiKeySliceQuery newQuery = new MultiKeySliceQuery(this);
        newQuery.setLimit(newLimit);
        return newQuery;
    }

    /**
     * Reads the entries of the keys one after the other, in the order of the keys and no more than the limit of them,
     * as this method always has. {@link #execute(BackendTransaction, boolean)} can read keys together.
     */
    public List<EntryList> execute(final BackendTransaction tx) {
        return execute(tx, false);
    }

    /**
     * Reads the entries of the keys, in the order of the keys and no more than the limit of them.
     * <p>
     * Where the storage backend has multi-key queries and reading together is allowed, keys are read together when
     * that reads no key which reading them one after the other would not: without a limit, which needs every key
     * anyway, and with a limit when each key holds at most one entry, as many keys per call as the limit can still take.
     * A call reads at most {@link #MAX_KEYS_PER_READ} keys. Otherwise the keys are read one after the other, so that
     * reading stops at the key which reaches the limit.
     *
     * @param readKeysTogether whether keys may be read together: the multi-query setting of the graph transaction,
     *                         which {@code query.batch.enabled} gives it unless the transaction sets its own
     */
    public List<EntryList> execute(final BackendTransaction tx, final boolean readKeysTogether) {
        if (readKeysTogether && queries.size() > 1 && sharesOneSlice && tx.hasMultiKeyIndexQueries()
            && (!hasLimit() || (atMostOneEntryPerKey && getLimit() > 0))) {
            return executeTogether(tx);
        }
        return executeOneByOne(tx);
    }

    private List<EntryList> executeOneByOne(final BackendTransaction tx) {
        int total = 0;
        final List<EntryList> result = new ArrayList<>(Math.min(getLimit(), queries.size()));
        for (KeySliceQuery ksq : queries) {
            EntryList next =tx.indexQuery(ksq.updateLimit(getLimit()-total));
            result.add(next);
            total+=next.size();
            if (total>=getLimit() && hasLimit()) break;
        }
        return result;
    }

    private List<EntryList> executeTogether(final BackendTransaction tx) {
        final KeySliceQuery first = queries.get(0);
        final SliceQuery slice = new SliceQuery(first.getSliceStart(), first.getSliceEnd());
        final List<EntryList> result = new ArrayList<>(Math.min(getLimit(), queries.size()));
        int total = 0;
        int next = 0;
        while (next < queries.size()) {
            //Without a limit every key is read, so the calls take all of them in turn. With a limit each key holds at
            //most one entry, so a call for as many keys as the limit can still take reads none which reading one after
            //the other would not
            final long wanted = hasLimit() ? getLimit() - total : queries.size() - next;
            final int end = (int) Math.min(queries.size(), next + Math.min(wanted, MAX_KEYS_PER_READ));
            //A limit of zero reads one key after the other, so each call takes at least one key
            Preconditions.checkState(end > next, "No key to read with %s wanted", wanted);
            final Map<StaticBuffer, EntryList> read = tx.indexStoreMultiQuery(distinctKeys(next, end),
                hasLimit() ? slice.updateLimit(getLimit() - total) : slice);
            for (int i = next; i < end; i++) {
                EntryList entries = read.get(queries.get(i).getKey());
                if (entries == null) {
                    entries = EntryList.EMPTY_LIST;
                } else if (hasLimit() && entries.size() > getLimit() - total) {
                    //No more than reading one after the other asks for this key
                    entries = EntryArrayList.of(entries.subList(0, getLimit() - total));
                }
                result.add(entries);
                total += entries.size();
                if (hasLimit() && total >= getLimit()) {
                    return result;
                }
            }
            next = end;
        }
        return result;
    }

    //The keys of the queries in the range, each once: a key which recurs is read once for all of its queries
    private List<StaticBuffer> distinctKeys(int from, int to) {
        final Set<StaticBuffer> keys = new LinkedHashSet<>();
        for (int i = from; i < to; i++) {
            keys.add(queries.get(i).getKey());
        }
        return new ArrayList<>(keys);
    }

    private static boolean sharesOneSlice(List<KeySliceQuery> queries) {
        final KeySliceQuery first = queries.get(0);
        for (KeySliceQuery query : queries) {
            if (!query.getSliceStart().equals(first.getSliceStart()) || !query.getSliceEnd().equals(first.getSliceEnd())) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hash(queries, getLimit());
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        else if (other == null) return false;
        else if (!getClass().isInstance(other)) return false;
        MultiKeySliceQuery oth = (MultiKeySliceQuery) other;
        return getLimit()==oth.getLimit() && queries.equals(oth.queries);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder("multiKSQ[");
        builder.append(queries.size()).append("]");
        if (hasLimit()) builder.append("@").append(getLimit());
        builder.append("{");
        for (int i = 0; i < queries.size(); i++) {
            if (i > 0) {
                builder.append(",");
            }
            builder.append(queries.get(i));
        }
        builder.append("}");
        return builder.toString();
    }
}
