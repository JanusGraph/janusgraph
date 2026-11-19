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

import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;
import org.janusgraph.diskstorage.es.compat.ES7Compat;
import org.janusgraph.diskstorage.es.rest.RestSearchResponse;
import org.janusgraph.diskstorage.indexing.IndexProvider;
import org.janusgraph.diskstorage.indexing.RawQuery;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

// The edge sort-key field travels in the _source of each hit: requested by name, and read back as a result which
// carries it
public class ElasticSearchSortKeyFieldTest {

    @Test
    public void aRequestForTheSortKeyAsksForThatFieldOfTheSource() {
        final ES7Compat compat = new ES7Compat();
        final ElasticSearchRequest withSortKey = new ElasticSearchRequest();
        withSortKey.setFields(Collections.singletonList(IndexProvider.EDGE_SORT_KEY_FIELD));
        assertEquals(Collections.singletonList(IndexProvider.EDGE_SORT_KEY_FIELD),
            compat.createRequestBody(withSortKey, null).get("_source"));

        final ElasticSearchRequest withoutSource = new ElasticSearchRequest();
        withoutSource.setDisableSourceRetrieval(true);
        assertEquals(false, compat.createRequestBody(withoutSource, null).get("_source"));

        assertFalse(compat.createRequestBody(new ElasticSearchRequest(), null).containsKey("_source"));
        assertTrue(compat.getIndexFeatures().supportsEdgeSortKeyField());
    }

    @Test
    public void aHitWithTheSortKeyInItsSourceCarriesIt() throws Exception {
        final String json = "{\"took\":1,\"hits\":{\"hits\":["
            + "{\"_id\":\"doc1\",\"_score\":1.5,\"_source\":{\"" + IndexProvider.EDGE_SORT_KEY_FIELD + "\":\"0a0b\"}},"
            + "{\"_id\":\"doc2\",\"_score\":1.0,\"_source\":{}},"
            + "{\"_id\":\"doc3\",\"_score\":0.5}]}}";
        final RestSearchResponse response = new ObjectMapper().readValue(json, RestSearchResponse.class);
        final List<RawQuery.Result<String>> results = response.getResults().collect(Collectors.toList());
        assertEquals(3, results.size());

        assertTrue(results.get(0) instanceof RestSearchResponse.SortKeyedResult);
        assertEquals("doc1", results.get(0).getResult());
        assertEquals(1.5, results.get(0).getScore(), 0.0);
        assertEquals("0a0b", ((RestSearchResponse.SortKeyedResult) results.get(0)).getSortKey());

        for (final RawQuery.Result<String> result : results.subList(1, 3)) {
            assertFalse(result instanceof RestSearchResponse.SortKeyedResult, result.getResult());
        }
        final Map<String, Double> scores = results.stream()
            .collect(Collectors.toMap(RawQuery.Result::getResult, RawQuery.Result::getScore));
        assertEquals(0.5, scores.get("doc3"), 0.0);
    }
}
