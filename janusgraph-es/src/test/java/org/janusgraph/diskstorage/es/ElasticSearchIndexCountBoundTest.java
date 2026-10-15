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

package org.janusgraph.diskstorage.es;

import org.janusgraph.graphdb.query.BaseQuery;
import org.janusgraph.graphdb.query.Query;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The bound of a count is the query's offset and limit, and there is none without a limit or where the two don't fit
 * an int; a count with a limit of 0 asks Elasticsearch nothing.
 */
public class ElasticSearchIndexCountBoundTest {

    private static Query withLimit(int limit) {
        return new BaseQuery(limit);
    }

    @Test
    public void shouldBoundACountByItsOffsetAndLimit() {
        assertEquals(15, ElasticSearchIndex.countBound(5, withLimit(10)));
        assertEquals(10, ElasticSearchIndex.countBound(0, withLimit(10)));
    }

    @Test
    public void shouldCountNothingWithALimitOfZero() {
        assertTrue(ElasticSearchIndex.countsNothing(withLimit(0)));
        assertFalse(ElasticSearchIndex.countsNothing(withLimit(1)));
        assertFalse(ElasticSearchIndex.countsNothing(new BaseQuery()));
    }

    @Test
    public void shouldNotBoundACountWithoutALimit() {
        assertEquals(0, ElasticSearchIndex.countBound(5, new BaseQuery()));
    }

    @Test
    public void shouldNotBoundACountWhoseBoundDoesNotFitAnInt() {
        assertEquals(0, ElasticSearchIndex.countBound(1, withLimit(Integer.MAX_VALUE - 1)));
        assertEquals(Integer.MAX_VALUE - 1, ElasticSearchIndex.countBound(0, withLimit(Integer.MAX_VALUE - 1)));
    }
}
