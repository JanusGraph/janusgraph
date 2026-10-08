// Copyright 2023 JanusGraph Authors
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

package org.janusgraph.graphdb.database.util;

import org.apache.commons.lang3.StringUtils;
import org.janusgraph.core.JanusGraphEdge;
import org.janusgraph.core.JanusGraphElement;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphRelation;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.core.JanusGraphVertexProperty;
import org.janusgraph.core.log.Change;
import org.janusgraph.core.log.ChangeProcessor;
import org.janusgraph.core.log.LogProcessorBuilder;
import org.janusgraph.core.log.LogProcessorFramework;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.graphdb.database.IndexSerializer;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.database.cache.CacheInvalidationService;
import org.janusgraph.graphdb.database.index.IndexUpdate;
import org.janusgraph.graphdb.internal.ElementLifeCycle;
import org.janusgraph.graphdb.internal.InternalRelation;
import org.janusgraph.graphdb.internal.InternalVertex;
import org.janusgraph.graphdb.relations.StandardVertexProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A helper util class for global db-cache invalidation log processor building logic.
 *
 * Usage example:
 * LogProcessorCacheInvalidationUtil.startLogProcessorCacheInvalidation("myTransactionLogIdentifier", (StandardJanusGraph) graph);
 *
 * Notice, all transactions in other JanusGraph instances will need to use the same logIdentifier (i.e. "myTransactionLogIdentifier");
 * I.e.: `graph.buildTransaction().logIdentifier("yourTransactionLogIdentifier").start()`
 *
 */
public class LogProcessorCacheInvalidationUtil {

    private static final Logger log =
        LoggerFactory.getLogger(LogProcessorCacheInvalidationUtil.class);

    public static void startLogProcessorCacheInvalidation(String logIdentifier, StandardJanusGraph graph){
        LogProcessorFramework logProcessorFramework = JanusGraphFactory.openTransactionLog(graph);
        startLogProcessorCacheInvalidation(logProcessorFramework, logIdentifier, graph);
    }

    public static void startLogProcessorCacheInvalidation(LogProcessorFramework logProcessorFramework, String logIdentifier, StandardJanusGraph graph){
        LogProcessorBuilder logProcessorBuilder = logProcessorFramework
            .addLogProcessor(logIdentifier)
            .setStartTimeNow();
        logProcessorBuilder = addCacheInvalidationChangeProcessorToLogBuilder(logProcessorBuilder, graph);
        logProcessorBuilder.build();
        log.debug("db-cache invalidation log processor is started for log identifier {}", logIdentifier);
    }

    public static LogProcessorBuilder addCacheInvalidationChangeProcessorToLogBuilder(LogProcessorBuilder logProcessorBuilder, StandardJanusGraph graph) {
        IndexSerializer indexSerializer = graph.getIndexSerializer();
        CacheInvalidationService cacheInvalidationService = graph.getDBCacheInvalidationService();
        ChangeProcessor cacheInvalidationChangeProcessor = buildCacheInvalidationChangeProcessor(indexSerializer, cacheInvalidationService);
        return logProcessorBuilder.addProcessor(cacheInvalidationChangeProcessor);
    }

    public static ChangeProcessor buildCacheInvalidationChangeProcessor(IndexSerializer indexSerializer, CacheInvalidationService cacheInvalidationService){
        return (tx, txId, changeState) -> {
            Set<JanusGraphVertex> addedVerticesToExpire = changeState.getVertices(Change.ADDED);
            Set<JanusGraphVertex> removedVerticesToExpire = changeState.getVertices(Change.REMOVED);
            Map<Long, JanusGraphElement> verticesMapToExpire = new HashMap<>((addedVerticesToExpire.size() + removedVerticesToExpire.size()) * 2);
            Set<StaticBuffer> indexStoreKeysToExpire = new HashSet<>();
            for(Set<JanusGraphVertex> verticesToExpire : Arrays.asList(addedVerticesToExpire, removedVerticesToExpire)){
                for(JanusGraphVertex vertexToExpire : verticesToExpire){
                    verticesMapToExpire.put((Long) vertexToExpire.id(), vertexToExpire);
                }
            }
            Map<Long, List<JanusGraphVertexProperty>> mutatedVertexPropertiesMap = new HashMap<>(verticesMapToExpire.size());
            for(JanusGraphRelation relation : changeState.getRelations(Change.ANY)){
                if(relation.isProperty()){
                    //TODO: check meta property
                    JanusGraphVertexProperty property = (JanusGraphVertexProperty) relation;
                    JanusGraphElement element = property.element();
                    mutatedVertexPropertiesMap.computeIfAbsent((Long) element.id(), vertexId -> new LinkedList<>()).add(property);
                    verticesMapToExpire.put((Long) element.id(), element);
                } else if(relation.isEdge()){
                    JanusGraphEdge edge = (JanusGraphEdge) relation;
                    verticesMapToExpire.put((Long) edge.outVertex().id(), edge.outVertex());
                    verticesMapToExpire.put((Long) edge.inVertex().id(), edge.inVertex());
                    if(edge instanceof InternalRelation){
                        Collection<IndexUpdate> indexUpdates = indexSerializer.getIndexUpdatesNoConstraints((InternalRelation) edge);
                        for(IndexUpdate indexUpdate : indexUpdates){
                            if(indexUpdate.getKey() instanceof StaticBuffer){
                                indexStoreKeysToExpire.add((StaticBuffer) indexUpdate.getKey());
                            }
                        }
                    }
                }
            }

            for(Map.Entry<Long, List<JanusGraphVertexProperty>> vertexPropertiesMutations : mutatedVertexPropertiesMap.entrySet()){
                JanusGraphElement possibleElementForInvalidation = verticesMapToExpire.get(vertexPropertiesMutations.getKey());
                if(!(possibleElementForInvalidation instanceof InternalVertex)){
                    continue;
                }
                InternalVertex internalVertex = (InternalVertex) possibleElementForInvalidation;

                Collection<InternalRelation> internalRelations = new ArrayList<>(vertexPropertiesMutations.getValue().size()*2);
                for(JanusGraphVertexProperty property : vertexPropertiesMutations.getValue()){
                    if(property instanceof InternalRelation){
                        internalRelations.add((InternalRelation) property);
                        StandardVertexProperty reverseProperty = new StandardVertexProperty(
                            property.longId(), property.propertyKey(), internalVertex, property.value(),
                            property.isRemoved() ? ElementLifeCycle.New : ElementLifeCycle.Removed);
                        internalRelations.add(reverseProperty);
                    }
                }

                // FIXME: If the updated vertex was removed before log processor had a chance to invalidate it, it will throw an exception
                // when querying this vertex.
                // We should probably use some wrapper to ensure it returns only the properties received here and don't query it
                Collection<IndexUpdate> indexUpdates = indexSerializer.getIndexUpdatesNoConstraints(internalVertex, internalRelations);

                for(IndexUpdate indexUpdate : indexUpdates){
                    if(indexUpdate.getKey() instanceof StaticBuffer){
                        indexStoreKeysToExpire.add((StaticBuffer) indexUpdate.getKey());
                    }
                }
            }

            for(StaticBuffer indexStoreKeyToExpire : indexStoreKeysToExpire){
                cacheInvalidationService.markKeyAsExpiredInIndexStore(indexStoreKeyToExpire);
            }

            for (Long verticesToExpire : verticesMapToExpire.keySet()){
                cacheInvalidationService.markVertexAsExpiredInEdgeStore(verticesToExpire);
            }

            if(log.isDebugEnabled()){
                log.debug("Expired {} index store keys and the next {} vertices [{}]",
                    indexStoreKeysToExpire.size(), verticesMapToExpire.size(), StringUtils.join(verticesMapToExpire, ","));
            }
        };
    }
}
