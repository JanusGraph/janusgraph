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

package org.janusgraph.graphdb.transaction.subquerycache;

import org.janusgraph.graphdb.query.Query;
import org.janusgraph.graphdb.query.graph.JointIndexQuery;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CaffeineSubqueryCacheTest {

    private static final List<Object> FIVE_IDS = Arrays.asList(1L, 2L, 3L, 4L, 5L);
    private static final List<Object> SIX_IDS = Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L);

    //An entry weighs its number of ids plus 2. The cache is maintained on the calling thread, so what it keeps is
    //settled when a put returns
    private static CaffeineSubqueryCache cacheOfWeight(long maximumWeight) {
        return new CaffeineSubqueryCache(maximumWeight);
    }

    //A subquery with a limit, which the cache keeps its result under without the limit
    private static JointIndexQuery.Subquery query(JointIndexQuery.Subquery withoutLimit, int limit) {
        final JointIndexQuery.Subquery query = mock(JointIndexQuery.Subquery.class);
        when(query.getLimit()).thenReturn(limit);
        when(query.updateLimit(0)).thenReturn(withoutLimit);
        return query;
    }

    @Test
    public void shouldKeepAResultSetAsLargeAsItsMaximumCachedResultSize() {
        final CaffeineSubqueryCache cache = cacheOfWeight(7);
        assertEquals(5, cache.maximumCachedResultSize());
        final JointIndexQuery.Subquery query = query(mock(JointIndexQuery.Subquery.class), Query.NO_LIMIT);
        cache.put(query, FIVE_IDS);
        assertEquals(FIVE_IDS, cache.getIfPresent(query));
    }

    //Which is why a put leaves a longer list out
    @Test
    public void shouldDropAnEntryHeavierThanTheMaximumWeight() {
        final CaffeineSubqueryCache cache = cacheOfWeight(7);
        final JointIndexQuery.Subquery withoutLimit = mock(JointIndexQuery.Subquery.class);
        cache.put(withoutLimit, new SubsetSubqueryCache.SubqueryResult(SIX_IDS, Query.NO_LIMIT));
        assertNull(cache.get(withoutLimit));
    }

    //The longer list would replace the result held for the same query under a smaller limit, which the cache keeps
    //under the query without its limit too, and be dropped with it
    @Test
    public void shouldKeepTheResultOfASmallerLimitWhenALongerListIsPut() {
        final CaffeineSubqueryCache cache = cacheOfWeight(7);
        final JointIndexQuery.Subquery withoutLimit = mock(JointIndexQuery.Subquery.class);
        final JointIndexQuery.Subquery limited = query(withoutLimit, 2);
        cache.put(limited, Arrays.asList(1L, 2L));
        cache.put(query(withoutLimit, Query.NO_LIMIT), SIX_IDS);
        assertEquals(Arrays.asList(1L, 2L), cache.getIfPresent(limited));
    }

    //The other indexes of a joint query load their results through the cache: a list longer than the cache keeps is
    //handed back whole but not kept, and leaves the result of a smaller limit in place as well
    @Test
    public void shouldHandBackALongerListLoadedThroughTheCacheWithoutKeepingIt() throws Exception {
        final CaffeineSubqueryCache cache = cacheOfWeight(7);
        final JointIndexQuery.Subquery withoutLimit = mock(JointIndexQuery.Subquery.class);
        final JointIndexQuery.Subquery limited = query(withoutLimit, 2);
        cache.put(limited, Arrays.asList(1L, 2L));
        assertEquals(SIX_IDS, cache.get(query(withoutLimit, Query.NO_LIMIT), () -> SIX_IDS));
        assertEquals(Arrays.asList(1L, 2L), cache.getIfPresent(limited));
    }

    @Test
    public void shouldKeepOnlyEmptyResultSetsWhenTheMaximumWeightIsThatOfAnEmptyEntry() {
        final CaffeineSubqueryCache cache = cacheOfWeight(2);
        assertEquals(0, cache.maximumCachedResultSize());
        final JointIndexQuery.Subquery query = query(mock(JointIndexQuery.Subquery.class), Query.NO_LIMIT);
        cache.put(query, Collections.emptyList());
        assertEquals(Collections.emptyList(), cache.getIfPresent(query));
    }

    @Test
    public void shouldKeepNoResultSetWhenNotEvenAnEmptyOneWeighsLittleEnough() {
        assertTrue(cacheOfWeight(1).maximumCachedResultSize() < 0);
        assertTrue(cacheOfWeight(0).maximumCachedResultSize() < 0);
    }

    @Test
    public void shouldBoundTheResultSetSizeByTheLongestList() {
        assertEquals(Integer.MAX_VALUE, cacheOfWeight(Long.MAX_VALUE).maximumCachedResultSize());
    }
}
