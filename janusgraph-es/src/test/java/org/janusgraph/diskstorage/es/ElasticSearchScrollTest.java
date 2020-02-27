// Copyright 2020 JanusGraph Authors
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

import io.vavr.collection.List;
import org.janusgraph.diskstorage.indexing.RawQuery;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.NoSuchElementException;

import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.easymock.EasyMock.expect;

public class ElasticSearchScrollTest {
    @Test
    public void testShortResultSetDeletesScroll() throws IOException {
        String SCROLL_NAME = "scroll-1";
        ElasticSearchResponse response = new ElasticSearchResponse();
        response.setScrollId(SCROLL_NAME);
        response.setTotal(0);
        response.setResults(Collections.emptyList());

        ElasticSearchClient client = mock(ElasticSearchClient.class);
        client.deleteScroll(SCROLL_NAME);
        replay(client);

        new ElasticSearchScroll(client, response, 5);

        verify(client);
    }

    @Test
    public void testLongResultSetDoesNotDeleteScrollOnConstruction() throws IOException {
        List x = List.of(new RawQuery.Result<String>("", 0.0),
            new RawQuery.Result<String>("", 0.0),
            new RawQuery.Result<String>("", 0.0),
            new RawQuery.Result<String>("", 0.0),
            new RawQuery.Result<String>("", 0.0));
        String SCROLL_NAME = "scroll-2";
        ElasticSearchResponse response = new ElasticSearchResponse();
        response.setScrollId(SCROLL_NAME);
        response.setTotal(x.size());
        response.setResults(x.asJava());

        ElasticSearchClient client = mock(ElasticSearchClient.class);
        replay(client);

        new ElasticSearchScroll(client, response, x.length());

        verify(client);
    }

    @Test
    public void testLongResultSetDeleteScrollOnLastShortPage() throws IOException {
        List x1 = List.of(new RawQuery.Result<String>("1", 10.0),
            new RawQuery.Result<String>("2", 10.0),
            new RawQuery.Result<String>("3", 10.0),
            new RawQuery.Result<String>("4", 10.0),
            new RawQuery.Result<String>("5", 10.0));

        List x2 = List.of(new RawQuery.Result<String>("6", 10.0),
            new RawQuery.Result<String>("7", 10.0));

        String SCROLL_NAME = "scroll-2";
        ElasticSearchResponse r1 = new ElasticSearchResponse();
        r1.setScrollId(SCROLL_NAME);
        r1.setTotal(x1.size());
        r1.setResults(x1.asJava());

        ElasticSearchResponse r2 = new ElasticSearchResponse();
        r2.setScrollId(SCROLL_NAME);
        r2.setTotal(x2.size());
        r2.setResults(x2.asJava());

        ElasticSearchClient client = mock(ElasticSearchClient.class);

        expect(client.search(SCROLL_NAME)).andReturn(r2);
        client.deleteScroll(SCROLL_NAME);
        replay(client);

        ElasticSearchScroll scroll = new ElasticSearchScroll(client, r1, 5);
        for (int i=0 ; i<6 ; i++) {
            scroll.hasNext();
            scroll.next();
        }

        verify(client);
    }

    @Test
    public void testScrollThrowNoSuchElementWhenReadingPastEnd() throws IOException {
        List x = List.of(new RawQuery.Result<String>("", 0.0),
            new RawQuery.Result<String>("", 0.0),
            new RawQuery.Result<String>("", 0.0),
            new RawQuery.Result<String>("", 0.0));
        String SCROLL_NAME = "scroll-2";
        ElasticSearchResponse response = new ElasticSearchResponse();
        response.setScrollId(SCROLL_NAME);
        response.setTotal(x.size());
        response.setResults(x.asJava());

        ElasticSearchClient client = mock(ElasticSearchClient.class);
        client.deleteScroll(SCROLL_NAME);
        replay(client);

        ElasticSearchScroll scroll = new ElasticSearchScroll(client, response, 5);
        for (int i=0 ; i<x.length(); i++) {
            scroll.hasNext();
            scroll.next();
        }
        Assertions.assertThrows(NoSuchElementException.class, () -> scroll.next());

        verify(client);
    }
}
