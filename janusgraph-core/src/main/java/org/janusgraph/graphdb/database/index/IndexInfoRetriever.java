// Copyright 2022 JanusGraph Authors
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

package org.janusgraph.graphdb.database.index;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.JanusGraphException;
import org.janusgraph.core.schema.Mapping;
import org.janusgraph.diskstorage.indexing.IndexProvider;
import org.janusgraph.diskstorage.indexing.KeyInformation;
import org.janusgraph.diskstorage.indexing.StandardKeyInformation;
import org.janusgraph.graphdb.database.util.IndexRecordUtil;
import org.janusgraph.graphdb.internal.ElementCategory;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.janusgraph.graphdb.types.MixedIndexType;
import org.janusgraph.graphdb.types.ParameterIndexField;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class IndexInfoRetriever implements KeyInformation.Retriever {

    // The field that holds the encoded sort key of an edge document, see IndexSerializer.storesEdgeSortKey
    public static final KeyInformation EDGE_SORT_KEY_INFORMATION =
        new StandardKeyInformation(String.class, Cardinality.SINGLE, Mapping.STRING.asParameter());

    private final StandardJanusGraphTx transaction;

    public IndexInfoRetriever(StandardJanusGraphTx tx) {
        Preconditions.checkNotNull(tx);
        transaction=tx;
    }

    @Override
    public KeyInformation.IndexRetriever get(final String index) {
        return new KeyInformation.IndexRetriever() {

            final Map<String,KeyInformation.StoreRetriever> indexes = new ConcurrentHashMap<>();

            @Override
            public KeyInformation get(String store, String key) {
                return get(store).get(key);
            }

            @Override
            public KeyInformation.StoreRetriever get(final String store) {
                if (indexes.get(store)==null) {
                    Preconditions.checkNotNull(transaction,"Retriever has not been initialized");
                    final MixedIndexType extIndex = IndexRecordUtil.getMixedIndex(store, transaction);
                    assert extIndex.getBackingIndexName().equals(index);
                    final ImmutableMap.Builder<String,KeyInformation> b = ImmutableMap.builder();
                    for (final ParameterIndexField field : extIndex.getFieldKeys()) b.put(IndexRecordUtil.key2Field(field),IndexRecordUtil.getKeyInformation(field));
                    ImmutableMap<String,KeyInformation> infoMap;
                    try {
                        infoMap = b.build();
                    } catch (IllegalArgumentException e) {
                        throw new JanusGraphException("Duplicate index field names found, likely you have multiple properties mapped to the same index field", e);
                    }
                    final boolean edgeIndex = extIndex.getElement() == ElementCategory.EDGE
                        && !infoMap.containsKey(IndexProvider.EDGE_SORT_KEY_FIELD);
                    final KeyInformation.StoreRetriever storeRetriever = key -> {
                        final KeyInformation info = infoMap.get(key);
                        //Edge documents may carry their sort key, which is not a property key of the index
                        if (info == null && edgeIndex && IndexProvider.EDGE_SORT_KEY_FIELD.equals(key)) {
                            return EDGE_SORT_KEY_INFORMATION;
                        }
                        return info;
                    };
                    indexes.put(store,storeRetriever);
                }
                return indexes.get(store);
            }

            @Override
            public void invalidate(final String store) {
                indexes.remove(store);
            }
        };
    }
}
