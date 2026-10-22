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

package org.janusgraph.graphdb.query.index;

import org.janusgraph.core.Cardinality;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.Mapping;
import org.janusgraph.diskstorage.indexing.IndexFeatures;
import org.janusgraph.diskstorage.indexing.IndexInformation;
import org.janusgraph.diskstorage.indexing.KeyInformation;
import org.janusgraph.graphdb.database.IndexSerializer;
import org.janusgraph.graphdb.internal.Order;
import org.janusgraph.graphdb.internal.OrderList;
import org.janusgraph.graphdb.query.JanusGraphPredicate;
import org.janusgraph.graphdb.types.MixedIndexType;
import org.janusgraph.graphdb.types.ParameterIndexField;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

public class IndexSelectionUtilTest {

    private static final String BACKING_INDEX = "search";

    //An index covers an order only where its provider can sort by each of the order's keys, wherever in the order a
    //key which it can't sort by comes; an index provider which doesn't tell, as one written before
    //IndexInformation.supportsOrdering, is given an order by any key which the index holds
    @Test
    public void anIndexCoversAnOrderOnlyByTheKeysWhichItsProviderCanSortBy() {
        final MixedIndexType index = mock(MixedIndexType.class);
        doReturn(BACKING_INDEX).when(index).getBackingIndexName();
        final PropertyKey kind = indexedKey(index, "kind", Mapping.STRING);
        final PropertyKey title = indexedKey(index, "title", Mapping.TEXT);
        final IndexSerializer sortsAllButText = serializerOf(new Provider() {
            @Override
            public boolean supportsOrdering(KeyInformation information) {
                return Mapping.getMapping(information) != Mapping.TEXT;
            }
        });
        final IndexSerializer tellsNothing = serializerOf(new Provider());

        assertTrue(IndexSelectionUtil.indexCoversOrder(index, orderBy(kind), sortsAllButText));
        for (final OrderList orders : Arrays.asList(orderBy(title), orderBy(kind, title), orderBy(title, kind))) {
            assertFalse(IndexSelectionUtil.indexCoversOrder(index, orders, sortsAllButText), orders.toString());
            assertTrue(IndexSelectionUtil.indexCoversOrder(index, orders, tellsNothing), orders.toString());
        }
    }

    private static PropertyKey indexedKey(MixedIndexType index, String name, Mapping mapping) {
        final PropertyKey key = mock(PropertyKey.class, name);
        doReturn(String.class).when(key).dataType();
        doReturn(Cardinality.SINGLE).when(key).cardinality();
        doReturn(true).when(index).indexesKey(key);
        doReturn(ParameterIndexField.of(key, mapping.asParameter())).when(index).getField(key);
        return key;
    }

    private static OrderList orderBy(PropertyKey... keys) {
        final OrderList orders = new OrderList();
        for (final PropertyKey key : keys) {
            orders.add(key, Order.ASC);
        }
        return orders;
    }

    private static IndexSerializer serializerOf(IndexInformation provider) {
        return new IndexSerializer(null, null, null, Collections.singletonMap(BACKING_INDEX, provider), false);
    }

    private static class Provider implements IndexInformation {

        @Override
        public boolean supports(KeyInformation information, JanusGraphPredicate janusgraphPredicate) {
            return true;
        }

        @Override
        public boolean supports(KeyInformation information) {
            return true;
        }

        @Override
        public String mapKey2Field(String key, KeyInformation information) {
            return key;
        }

        @Override
        public IndexFeatures getFeatures() {
            return new IndexFeatures.Builder().build();
        }
    }
}
