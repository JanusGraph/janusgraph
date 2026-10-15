// Copyright 2026 JanusGraph Authors
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

package org.janusgraph.diskstorage.cql;

import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.cql.util.CQLSliceQueryUtil;
import org.janusgraph.diskstorage.keycolumnvalue.KeysQueriesGroup;
import org.janusgraph.diskstorage.keycolumnvalue.SliceQuery;
import org.janusgraph.diskstorage.util.BufferUtil;
import org.janusgraph.graphdb.query.Query;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Queries which each read a single column are read in groups, one query per key or keys group for each group: as many
 * groups as it takes, each no larger than the slice grouping limit and the limit its queries share. It lives in this
 * package, as the CI jobs of the CQL backend run the tests of this package.
 */
public class CQLSliceQueryUtilTest {

    private static final int SLICE_GROUPING_LIMIT = 20;

    //A query for one column, as a property of a single cardinality key is read
    private static SliceQuery column(int column, int limit) {
        final StaticBuffer start = BufferUtil.getIntBuffer(column);
        final SliceQuery query = new SliceQuery(start, BufferUtil.nextBiggerBuffer(start)).setDirectColumnByStartOnlyAllowed(true);
        return limit == Query.NO_LIMIT ? query : query.setLimit(limit);
    }

    private static List<SliceQuery> columns(int from, int to, int limit) {
        final List<SliceQuery> queries = new ArrayList<>();
        for (int column = from; column < to; column++) {
            queries.add(column(column, limit));
        }
        return queries;
    }

    private static QueryGroups group(List<SliceQuery> queries) {
        return CQLSliceQueryUtil.getQueriesGroupedByDirectEqualityQueries(
            new KeysQueriesGroup<>(Collections.singletonList(BufferUtil.getIntBuffer(1)), queries), SLICE_GROUPING_LIMIT);
    }

    private static List<Integer> sizes(QueryGroups groups, int limit) {
        return groups.getDirectEqualityGroups().stream().filter(group -> group.getLimit() == limit)
            .map(group -> group.getQueries().size()).collect(Collectors.toList());
    }

    @Test
    public void shouldReadSingleColumnsInGroupsOfTheSliceGroupingLimit() {
        final List<SliceQuery> queries = columns(0, 50, Query.NO_LIMIT);

        final QueryGroups groups = group(queries);

        //Rather than one group of 20 and 30 queries of their own
        assertEquals(Arrays.asList(20, 20, 10), sizes(groups, Query.NO_LIMIT));
        assertTrue(groups.getSeparateRangeQueries().isEmpty());
        assertEquals(queries, groups.getDirectEqualityGroups().stream()
            .flatMap(group -> group.getQueries().stream()).collect(Collectors.toList()));
    }

    //A group's query asks for as many columns as the limit, so a larger group could cut off a column of one of its queries
    @Test
    public void shouldGroupNoMoreQueriesThanTheirLimit() {
        final QueryGroups groups = group(columns(0, 7, 3));

        assertEquals(Arrays.asList(3, 3, 1), sizes(groups, 3));
        assertTrue(groups.getSeparateRangeQueries().isEmpty());
    }

    @Test
    public void shouldGroupQueriesOfDifferentLimitsApart() {
        final List<SliceQuery> queries = new ArrayList<>(columns(0, 25, Query.NO_LIMIT));
        queries.addAll(columns(25, 28, 1));

        final QueryGroups groups = group(queries);

        assertEquals(Arrays.asList(20, 5), sizes(groups, Query.NO_LIMIT));
        assertEquals(Arrays.asList(1, 1, 1), sizes(groups, 1));
        assertEquals(5, groups.getDirectEqualityGroups().size());
    }

    @Test
    public void shouldReadRangesOfColumnsSeparately() {
        final SliceQuery range = new SliceQuery(BufferUtil.getIntBuffer(0), BufferUtil.getIntBuffer(100));
        final List<SliceQuery> queries = new ArrayList<>(columns(0, 3, Query.NO_LIMIT));
        queries.add(range);

        final QueryGroups groups = group(queries);

        assertEquals(Collections.singletonList(3), sizes(groups, Query.NO_LIMIT));
        assertEquals(Collections.singletonList(range), groups.getSeparateRangeQueries());
    }
}
