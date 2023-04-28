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

package org.janusgraph.graphdb.database.cache;

import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.keycolumnvalue.cache.KCVSCache;
import org.janusgraph.graphdb.idmanagement.IDManager;

import java.util.Collections;

public class KCVSCacheInvalidationService implements CacheInvalidationService{

    private final KCVSCache edgeStore;
    private final KCVSCache indexStore;

    private final IDManager idManager;

    public KCVSCacheInvalidationService(KCVSCache edgeStore, KCVSCache indexStore, IDManager idManager) {
        this.edgeStore = edgeStore;
        this.indexStore = indexStore;
        this.idManager = idManager;
    }

    @Override
    public void markVertexAsExpiredInEdgeStore(Object vertexId) {
        StaticBuffer vertexIdKey = idManager.getKey(vertexId);
        markKeyAsExpiredInEdgeStore(vertexIdKey);
    }

    @Override
    public void markKeyAsExpiredInEdgeStore(StaticBuffer key) {
        edgeStore.invalidate(key, Collections.emptyList());
    }

    @Override
    public void markKeyAsExpiredInIndexStore(StaticBuffer key) {
        indexStore.invalidate(key, Collections.emptyList());
    }

    @Override
    public void forceClearExpiredKeysInEdgeStoreCache() {
        edgeStore.forceClearExpiredCache();
    }

    @Override
    public void forceClearExpiredKeysInIndexStoreCache() {
        indexStore.forceClearExpiredCache();
    }

    @Override
    public void forceInvalidateVertexInEdgeStoreCache(Object vertexId) {
        markVertexAsExpiredInEdgeStore(vertexId);
        forceClearExpiredKeysInEdgeStoreCache();
    }

    @Override
    public void forceInvalidateVerticesInEdgeStoreCache(Iterable<Object> vertexIds) {
        for(Object vertexId : vertexIds){
            markVertexAsExpiredInEdgeStore(vertexId);
        }
        forceClearExpiredKeysInEdgeStoreCache();
    }

    @Override
    public void clearEdgeStoreCache() {
        edgeStore.clearCache();
    }

    @Override
    public void clearIndexStoreCache() {
        indexStore.clearCache();
    }

    @Override
    public void clearDBCache() {
        clearEdgeStoreCache();
        clearIndexStoreCache();
    }
}
