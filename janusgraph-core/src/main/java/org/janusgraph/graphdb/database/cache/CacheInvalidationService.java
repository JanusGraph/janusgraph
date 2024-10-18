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

import org.janusgraph.core.log.Change;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.graphdb.database.index.IndexUpdate;
import org.janusgraph.graphdb.internal.InternalRelation;
import org.janusgraph.graphdb.internal.InternalVertex;

import java.util.Collection;

/**
 * Cache invalidation service for manual JanusGraph database-level cache invalidation.
 * Use with great care because improper invalidation may result in stale data left in db-cache.
 * This service wraps two different caches called as `edgeStore` and `indexStore` which form a
 * single database-level cache (can be enabled via `cache.db-cache` configuration property).
 * When db-cache is disabled this service doesn't make any effective changes because the cache is
 * fully disabled in that case.
 * <p>
 * This class provides method for data invalidation for both `edgeStore` cache and `indexStore` cache
 * but invalidating entries in one cache doesn't invalidate entries in another cache.
 * Thus, for proper invalidation you need to invalidate necessary keys for both `edgeStore` and `indexStore`.
 * <p>
 * EdgeStore accepts keys where a key is an encoded vertex id. It's usually easy to invalidate EdgeStore because
 * it doesn't require you to know more information to form a key except knowing a vertex id.
 * <p>
 * IndexStore accepts keys where a key is an encoded IndexUpdate key. To form IndexUpdate key you need to know
 * the next information: vertex id, updated property name, previous property value and / or new property value.
 * Thus forming IndexUpdate key to invalidate IndexStore may be more complicated.
 * <p>
 * EdgeStore caches properties and edges for vertices.
 * IndexStore caches results for queries which use indices.
 * <p>
 * See JavaDoc on methods to learn how to properly form a `key` to invalidate cache in `edgeStore` or `indexStore`.
 */
public interface CacheInvalidationService {

    /**
     * Marks specific vertex as expired in `edgeStore` cache.
     * It will make sure that any retrieved properties and edges associated with this vertex will be invalidated in vertex cache.
     * <p>
     * Warning! This doesn't invalidate `indexStore` cache. Thus, any queries which are using indices may still return
     * stale data. See {@link #markKeyAsExpiredInIndexStore(StaticBuffer)} JavaDoc to learn how to invalidate data for
     * `indexStore`.
     *
     * @param vertexId vertex id to expire in `edgeStore` cache
     */
    void markVertexAsExpiredInEdgeStore(Object vertexId);

    /**
     * Marks specific key as expired in `edgeStore` cache.
     * It will make sure that any retrieved properties and edges associated with this key will be invalidated in vertex cache.
     * <p>
     * Warning! This doesn't invalidate `indexStore` cache. Thus, any queries which are using indices may still return
     * stale data. See {@link #markKeyAsExpiredInIndexStore(StaticBuffer)} JavaDoc to learn how to invalidate data for
     * `indexStore`.
     * <p>
     * {@link org.janusgraph.graphdb.idmanagement.IDManager#getKey(Object)} can be used to form a `key` from vertex id.
     * Alternatively, a method {@link #markVertexAsExpiredInEdgeStore(Object)} can be used which converts vertex id into
     * the `key` before passing the key to this method.
     * <p>
     * In case vertices invalidation is needed by processing transaction logs via {@link org.janusgraph.core.log.ChangeState}
     * then the method {@link org.janusgraph.core.log.ChangeState#getVertices(Change)} can be used to retrieve all
     * changed vertices and passing their ids to {@link #markVertexAsExpiredInEdgeStore(Object)}.
     *
     * @param key key to expire in `edgeStore` cache
     */
    void markKeyAsExpiredInEdgeStore(StaticBuffer key);

    /**
     * Marks specific key as expired in `indexStore` cache.
     * It will make sure that any retrieved data associated with this key will be invalidated in index cache.
     * <p>
     * Warning! This doesn't invalidate `edgeStore` cache. Thus, trying to return properties or edges for the vertex
     * may still return stale data. See {@link #markKeyAsExpiredInEdgeStore(StaticBuffer)} JavaDoc to learn how to invalidate
     * data for `edgeStore`.
     * <p>
     * `key` is the encoded key of {@link org.janusgraph.graphdb.database.index.IndexUpdate} which can be retrieved via
     * {@link IndexUpdate#getKey()}. To form the `IndexUpdate` it is possible to use
     * {@link org.janusgraph.graphdb.database.IndexSerializer#getIndexUpdates(InternalRelation, org.janusgraph.graphdb.types.TypeInspector)} or
     * {@link org.janusgraph.graphdb.database.IndexSerializer#getIndexUpdates(InternalVertex, Collection, org.janusgraph.graphdb.types.TypeInspector)}.
     * <p>
     * Usually updated vertices and relations (edges or properties) can be found in retrieved mutation logs which are
     * passed via {@link org.janusgraph.core.log.ChangeState} (described in `Transaction Log` documentation of JanusGraph).
     * <p>
     * It is also possible to trigger `indexStore` invalidation by forming a vertex and a property yourself.
     * For example, below method can be used to trigger `indexStore` invalidation for updated property if
     * previous value, new value, property name, and vertex id are known.
     *
     * <pre>
     * public void invalidateUpdatedVertexProperty(StandardJanusGraph graph, long vertexIdUpdated, String propertyNameUpdated, Object previousPropertyValue, Object newPropertyValue){
     *     JanusGraphTransaction tx = graph.newTransaction();
     *     JanusGraphManagement graphMgmt = graph.openManagement();
     *     PropertyKey propertyKey = graphMgmt.getPropertyKey(propertyNameUpdated);
     *     CacheVertex cacheVertex = new CacheVertex((StandardJanusGraphTx) tx, vertexIdUpdated, ElementLifeCycle.Loaded);
     *     StandardVertexProperty propertyPreviousVal = new StandardVertexProperty(propertyKey.longId(), propertyKey, cacheVertex, previousPropertyValue, ElementLifeCycle.Removed);
     *     StandardVertexProperty propertyNewVal = new StandardVertexProperty(propertyKey.longId(), propertyKey, cacheVertex, newPropertyValue, ElementLifeCycle.New);
     *     IndexSerializer indexSerializer = graph.getIndexSerializer();
     *
     *     Collection&lt;IndexUpdate&gt; indexUpdates = indexSerializer.getIndexUpdates(cacheVertex, Arrays.asList(propertyPreviousVal, propertyNewVal));
     *     CacheInvalidationService invalidationService = graph.getDBCacheInvalidationService();
     *
     *     for(IndexUpdate indexUpdate : indexUpdates){
     *         StaticBuffer keyToInvalidate = (StaticBuffer) indexUpdate.getKey();
     *         invalidationService.markKeyAsExpiredInIndexStore(keyToInvalidate);
     *     }
     *
     *     invalidationService.forceClearExpiredKeysInIndexStoreCache();
     *     invalidationService.forceInvalidateVertexInEdgeStoreCache(vertexIdUpdated);
     *
     *     graphMgmt.rollback();
     *     tx.rollback();
     * }
     * </pre>
     *
     * @param key key to expire in `indexStore` cache
     */
    void markKeyAsExpiredInIndexStore(StaticBuffer key);

    /**
     * Instead of waiting for a probabilistic invalidation it triggers all cached queries scan and invalidation in `edgeStore`.
     * This will remove any cached expired data.
     */
    void forceClearExpiredKeysInEdgeStoreCache();

    /**
     * Instead of waiting for a probabilistic invalidation it triggers all cached queries scan and invalidation in `indexStore`.
     * This will remove any cached expired data.
     */
    void forceClearExpiredKeysInIndexStoreCache();

    /**
     * Marks a vertex as expired in `edgeStore` cache ({@link #markVertexAsExpiredInEdgeStore(Object)}) and triggers force
     * clear of expired cache (i.e. {@link #forceClearExpiredKeysInEdgeStoreCache()})
     *
     * @param vertexId vertex id to invalidate in `edgeStore` cache
     */
    void forceInvalidateVertexInEdgeStoreCache(Object vertexId);

    /**
     * Marks vertices as expired in `edgeStore` cache ({@link #markVertexAsExpiredInEdgeStore(Object)}) and triggers force
     * clear of expired cache (i.e. {@link #forceClearExpiredKeysInEdgeStoreCache()})
     *
     * @param vertexIds vertex ids to invalidate in `edgeStore` cache
     */
    void forceInvalidateVerticesInEdgeStoreCache(Iterable<Object> vertexIds);

    /**
     * Clears `edgeStore` cache fully
     */
    void clearEdgeStoreCache();

    /**
     * Clears `indexStore` cache fully
     */
    void clearIndexStoreCache();

    /**
     * Clears both `edgeStore` cache and `indexStore` cache fully.
     * It is the same as calling {@link #clearEdgeStoreCache()} and {@link #clearIndexStoreCache()}
     */
    void clearDBCache();

}
