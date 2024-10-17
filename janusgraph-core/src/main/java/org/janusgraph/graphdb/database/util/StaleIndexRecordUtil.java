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

package org.janusgraph.graphdb.database.util;

import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphElement;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.JanusGraphIndex;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.BackendTransaction;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.indexing.IndexTransaction;
import org.janusgraph.diskstorage.util.HashingUtil;
import org.janusgraph.graphdb.database.EdgeSerializer;
import org.janusgraph.graphdb.database.IndexRecordEntry;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.database.index.IndexMutationType;
import org.janusgraph.graphdb.database.index.IndexUpdate;
import org.janusgraph.graphdb.database.management.ManagementSystem;
import org.janusgraph.graphdb.database.serialize.Serializer;
import org.janusgraph.graphdb.internal.ElementLifeCycle;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.janusgraph.graphdb.types.CompositeIndexType;
import org.janusgraph.graphdb.types.MixedIndexType;
import org.janusgraph.graphdb.types.vertices.JanusGraphSchemaVertex;
import org.janusgraph.graphdb.vertices.CacheVertex;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Helper class which simplifies stale index modification operations.
 * This class should normally be never used because it may cause index corruption problems in case it is used incorrectly.
 * <p>
 * In case more complicated index record modifications are needed, for example a case when a stale index have missing
 * added elements to the graph then it's possible to manually add or remove any index records of the index.
 * To do so you will need to use `BackendTransaction` directly.
 * Below is an example of direct `BackendTransaction` usage.
 * <p>
 * Vertex index record update:
 * <pre>
 * StandardJanusGraph graph = (StandardJanusGraph) JanusGraphFactory.open(configuration);
 * StandardJanusGraphTx tx = (StandardJanusGraphTx) graph.newTransaction();
 * ManagementSystem mgmt = (ManagementSystem) graph.openManagement();
 * // Let's say we want to remove non-existent vertex from a stale index.
 * // We will assume the next constraints:
 * // vertex id is 12345;
 * // Composite index name is: nameIndex
 * // There is a single indexed property: name
 * // Value of the vertex property is: HelloWorld
 * long vertexId = 12345L;
 * String compositeIndexName = "nameIndex";
 * String propertyKeyName = "name";
 * String value = "HelloWorld";
 * PropertyKey propertyKey = mgmt.getPropertyKey(propertyKeyName);
 * long propertyKeyId = propertyKey.longId();
 * IndexRecordEntry namePropertyIndexRecord = new IndexRecordEntry(propertyKeyId, value, propertyKey);
 * IndexRecordEntry[] fullIndexRecord = new IndexRecordEntry[]{namePropertyIndexRecord};
 * JanusGraphElement elementToBeRemoved = new CacheVertex(tx, vertexId, ElementLifeCycle.New);
 * JanusGraphIndex indexToBeUpdated = managementSystem.getGraphIndex(compositeIndexName);
 * JanusGraphSchemaVertex indexSchemaVertex = managementSystem.getSchemaVertex(indexToBeUpdated);
 * CompositeIndexType compositeIndexTypeToBeUpdated = (CompositeIndexType) indexSchemaVertex.asIndexType();
 * Serializer serializer = graph.getDataSerializer();
 * boolean hashKeys = graph.getIndexSerializer().isHashKeys();
 * HashingUtil.HashLength hashLength = graph.getIndexSerializer().getHashLength();
 * IndexUpdate&lt;StaticBuffer, Entry&gt; update = IndexRecordUtil.getCompositeIndexUpdate(
 *     compositeIndexTypeToBeUpdated,
 *     IndexMutationType.DELETE,
 *     fullIndexRecord,
 *     elementToBeRemoved,
 *     serializer,
 *     hashKeys,
 *     hashLength
 * );
 * BackendTransaction backendTransaction = tx.getTxHandle();
 * backendTransaction.mutateIndex(update.getKey(), Collections.emptyList(), Collections.singletonList(update.getEntry()));
 * transaction.commit();
 * tx.commit();
 * mgmt.rollback();
 * </pre>
 *
 * In case above you wanted to add index entry instead of removing it you would need to use {@link IndexMutationType#ADD IndexMutationType.ADD} instead
 * of {@link IndexMutationType#DELETE IndexMutationType.DELETE} as provide entries collection as a second parameter into
 * {@link BackendTransaction#mutateIndex mutateIndex} method instead of third parameter.
 * I.e. {@code backendTransaction.mutateIndex(update.getKey(), Collections.emptyList(), Collections.singletonList(update.getEntry()));}
 *
 * <p>
 * Edge index record update is currently more limited to Vertex index record update above as you will need to find `relationId`
 * as well as both ids of the connected vertices. It may be more challenging in case you don't have those elements in the graph.
 * Thus, below example shows how to remove index record of *existing* edge which leads to stale index. You shouldn't repeat
 * below steps unless you want to force remove existing edge index record.
 * <pre>
 * StandardJanusGraph graph = (StandardJanusGraph) JanusGraphFactory.open(configuration);
 * StandardJanusGraphTx tx = (StandardJanusGraphTx) graph.newTransaction();
 * ManagementSystem mgmt = (ManagementSystem) graph.openManagement();
 * // Let's say we want to remove existent edge from an index.
 * // We will assume the next constraints:
 * // Composite index name is: nameIndex
 * // There is a single indexed property: name
 * // Value of the vertex property is: HelloWorld
 * String compositeIndexName = "nameIndex";
 * String propertyKeyName = "name";
 * String value = "HelloWorld";
 * PropertyKey propertyKey = mgmt.getPropertyKey(propertyKeyName);
 * Edge edge = tx.traversal().E().has(propertyKeyName, value).next();
 * RelationIdentifier relationIdentifier = (RelationIdentifier) edge.id();
 * long relationId = relationIdentifier.getRelationId();
 * EdgeLabel edgeLabel = managementSystem.getEdgeLabel(edgeName);
 * IndexRecordEntry[] fullIndexRecord = new IndexRecordEntry[]{new IndexRecordEntry(relationId, value, propertyKey)};
 * InternalVertex internalVertex1 = (InternalVertex) edge.outVertex();
 * InternalVertex internalVertex2 = (InternalVertex) edge.inVertex();
 * JanusGraphElement elementToBeRemoved = new StandardEdge(relationId, edgeLabel, internalVertex1, internalVertex2, ElementLifeCycle.New);
 * JanusGraphIndex indexToBeUpdated = managementSystem.getGraphIndex(compositeIndexName);
 * JanusGraphSchemaVertex indexSchemaVertex = managementSystem.getSchemaVertex(indexToBeUpdated);
 * CompositeIndexType compositeIndexTypeToBeUpdated = (CompositeIndexType) indexSchemaVertex.asIndexType();
 * Serializer serializer = graph.getDataSerializer();
 * boolean hashKeys = graph.getIndexSerializer().isHashKeys();
 * HashingUtil.HashLength hashLength = graph.getIndexSerializer().getHashLength();
 * IndexUpdate&lt;StaticBuffer, Entry&gt; update = IndexRecordUtil.getCompositeIndexUpdate(
 *     compositeIndexTypeToBeUpdated,
 *     IndexMutationType.DELETE,
 *     fullIndexRecord,
 *     elementToBeRemoved,
 *     serializer,
 *     hashKeys,
 *     hashLength
 * );
 * BackendTransaction backendTransaction = tx.getTxHandle();
 * backendTransaction.mutateIndex(update.getKey(), Collections.emptyList(), Collections.singletonList(update.getEntry()));
 * transaction.commit();
 * tx.commit();
 * mgmt.rollback();
 * </pre>
 *
 */
public class StaleIndexRecordUtil {

    private static final String ANY_UPDATE_KEY = "_";
    private static final Object ANY_UPDATE_VALUE = new Object();

    /**
     * Force removes vertex record from a graph index.
     *
     * @param vertexId the vertex which should be removed from a specified index
     * @param indexRecordPropertyValues all property values of the index record
     * @param graph JanusGraph instance to be used to open graph management and new backend transaction for index removal.
     * @param graphIndexName name of the graph index for which to remove a record
     * @throws BackendException is thrown in case backend transaction cannot be mutated for any reason.
     */
    public static void forceRemoveVertexFromGraphIndex(Object vertexId,
                                                       Map<String, Object> indexRecordPropertyValues,
                                                       JanusGraph graph,
                                                       String graphIndexName) throws BackendException {

        ManagementSystem managementSystem = (ManagementSystem) graph.openManagement();

        try{
            JanusGraphIndex index = managementSystem.getGraphIndex(graphIndexName);
            PropertyKey[] propertyKeys = index.getFieldKeys();

            if(index.isCompositeIndex()){
                IndexRecordEntry[] indexRecord = toCompositeIndexRecord(propertyKeys, indexRecordPropertyValues);
                StandardJanusGraphTx tx = (StandardJanusGraphTx) graph.newTransaction();
                try{
                    JanusGraphElement elementToBeRemoved = new CacheVertex(tx, vertexId, ElementLifeCycle.New);
                    forceRemoveElementFromCompositeIndex(elementToBeRemoved, indexRecord, (StandardJanusGraph) graph, index, managementSystem);
                } finally {
                    tx.rollback();
                }

            } else if(index.isMixedIndex()){
                IndexRecordEntry[] indexRecord = toMixedIndexRecord(propertyKeys, indexRecordPropertyValues);
                forceRemoveElementFromMixedGraphIndex(vertexId, indexRecord, (StandardJanusGraph)  graph, index, managementSystem);
            }

        } finally {
            managementSystem.rollback();
        }
    }

    /**
     * Force removes element record from a graph index.
     * <p>
     * An example of using this method is below:
     * <pre>
     * StandardJanusGraph graph = (StandardJanusGraph) JanusGraphFactory.open(configuration);
     * StandardJanusGraphTx tx = (StandardJanusGraphTx) graph.newTransaction();
     * JanusGraphManagement mgmt = graph.openManagement();
     * // Let's say we want to remove non-existent vertex from a stale index.
     * // We will assume the next constraints:
     * // vertex id is 12345;
     * // Composite index name is: nameIndex
     * // There is a single indexed property: name
     * // Value of the vertex property is: HelloWorld
     * long vertexId = 12345L;
     * String compositeIndexName = "nameIndex";
     * String propertyKeyName = "name";
     * String value = "HelloWorld";
     * PropertyKey propertyKey = mgmt.getPropertyKey(propertyKeyName);
     * long propertyKeyId = propertyKey.longId();
     * IndexRecordEntry namePropertyIndexRecord = new IndexRecordEntry(propertyKeyId, value, propertyKey);
     * IndexRecordEntry[] fullIndexRecord = new IndexRecordEntry[]{namePropertyIndexRecord};
     * JanusGraphElement elementToBeRemoved = new CacheVertex(tx, vertexId, ElementLifeCycle.New);
     * // After the below method is executed index entry of the vertex 12345 should be removed from the index which
     * // effectively fixes permanent stale index inconsistency
     * StaleIndexRecordUtil.forceRemoveElementFromGraphIndex(
     *     elementToBeRemoved,
     *     fullIndexRecord,
     *     graph,
     *     compositeIndexName
     * );
     * tx.commit();
     * mgmt.rollback();
     * </pre>
     *
     * @param elementToRemoveFromIndex an element which should be removed.
     * @param indexRecord an ordered array or index record properties which represent this index record.
     * @param graph JanusGraph instance to be used to open graph management and new backend transaction for index removal.
     * @param graphIndexName index name of the graph index for which to delete a specified indexRecord.
     * @throws BackendException is thrown in case backend transaction cannot be mutated for any reason.
     */
    public static void forceRemoveElementFromGraphIndex(JanusGraphElement elementToRemoveFromIndex,
                                                        IndexRecordEntry[] indexRecord,
                                                        StandardJanusGraph graph,
                                                        String graphIndexName) throws BackendException {

        ManagementSystem managementSystem = (ManagementSystem) graph.openManagement();

        try{
            JanusGraphIndex index = managementSystem.getGraphIndex(graphIndexName);
            if(index.isCompositeIndex()){
                forceRemoveElementFromCompositeIndex(elementToRemoveFromIndex, indexRecord, graph, index, managementSystem);
            } else if(index.isMixedIndex()){
                forceRemoveElementFromMixedGraphIndex(elementToRemoveFromIndex.id(), indexRecord, graph, index, managementSystem);
            }
        } finally {
            managementSystem.rollback();
        }
    }

    /**
     * Force removes element record from a mixed index.
     *
     * @param elementId id of the element which should be removed from a specified index
     * @param indexRecordPropertyValues all property values of the index record
     * @param graph JanusGraph instance to be used to open graph management and new backend transaction for index removal.
     * @param mixedIndexName name of the mixed index for which to remove a record
     * @throws BackendException is thrown in case backend transaction cannot be mutated for any reason.
     */
    public static void forceRemoveElementFromMixedIndex(Object elementId,
                                                        Map<String, Object> indexRecordPropertyValues,
                                                        JanusGraph graph,
                                                        String mixedIndexName) throws BackendException {

        ManagementSystem managementSystem = (ManagementSystem) graph.openManagement();

        try{
            JanusGraphIndex index = managementSystem.getGraphIndex(mixedIndexName);
            PropertyKey[] propertyKeys = index.getFieldKeys();
            IndexRecordEntry[] indexRecord = toMixedIndexRecord(propertyKeys, indexRecordPropertyValues);
            forceRemoveElementFromMixedGraphIndex(elementId, indexRecord, (StandardJanusGraph)  graph, index, managementSystem);
        } finally {
            managementSystem.rollback();
        }
    }

    /**
     * Force removes element record from a mixed index.
     *
     * @param elementId id of the element which should be removed from a specified index
     * @param indexRecord an unordered array or index record properties which represent this index record.
     * @param graph JanusGraph instance to be used to open graph management and new backend transaction for index removal.
     * @param mixedIndexName name of the mixed index for which to remove a record
     * @throws BackendException is thrown in case backend transaction cannot be mutated for any reason.
     */
    public static void forceRemoveElementFromMixedIndex(Object elementId,
                                                        IndexRecordEntry[] indexRecord,
                                                        JanusGraph graph,
                                                        String mixedIndexName) throws BackendException {

        ManagementSystem managementSystem = (ManagementSystem) graph.openManagement();

        try{
            JanusGraphIndex index = managementSystem.getGraphIndex(mixedIndexName);
            forceRemoveElementFromMixedGraphIndex(elementId, indexRecord, (StandardJanusGraph)  graph, index, managementSystem);
        } finally {
            managementSystem.rollback();
        }
    }

    /**
     * Force removes element fully from a mixed index.
     *
     * @param elementId id of the element which should be removed from a specified index
     * @param graph JanusGraph instance to be used to open graph management and new backend transaction for index removal.
     * @param mixedIndexName name of the mixed index for which to remove a record
     * @throws BackendException is thrown in case backend transaction cannot be mutated for any reason.
     */
    public static void forceRemoveElementFromMixedIndex(Object elementId,
                                                        JanusGraph graph,
                                                        String mixedIndexName) throws BackendException {

        ManagementSystem managementSystem = (ManagementSystem) graph.openManagement();

        try{
            JanusGraphIndex index = managementSystem.getGraphIndex(mixedIndexName);
            forceRemoveElementFullyFromMixedGraphIndex(elementId, (StandardJanusGraph)  graph, index, managementSystem);
        } finally {
            managementSystem.rollback();
        }
    }

    private static void forceRemoveElementFromCompositeIndex(JanusGraphElement elementToRemoveFromIndex,
                                                             IndexRecordEntry[] indexRecord,
                                                             StandardJanusGraph graph,
                                                             JanusGraphIndex index,
                                                             ManagementSystem managementSystem) throws BackendException {

        verifyIndexIsComposite(index);

        Serializer serializer = graph.getDataSerializer();
        EdgeSerializer edgeSerializer = graph.getEdgeSerializer();
        boolean hashKeys = graph.getIndexSerializer().isHashKeys();
        HashingUtil.HashLength hashLength = graph.getIndexSerializer().getHashLength();

        JanusGraphSchemaVertex indexSchemaVertex = managementSystem.getSchemaVertex(index);

        CompositeIndexType compositeIndexType = (CompositeIndexType) indexSchemaVertex.asIndexType();

        StandardJanusGraphTx tx = (StandardJanusGraphTx) graph.newTransaction();
        BackendTransaction transaction = tx.getTxHandle();

        IndexUpdate<StaticBuffer, Entry> update = IndexRecordUtil.getCompositeIndexUpdate(
            compositeIndexType,
            IndexMutationType.DELETE,
            indexRecord,
            elementToRemoveFromIndex,
            serializer,
            tx,
            edgeSerializer,
            hashKeys,
            hashLength
        );

        try{
            transaction.mutateIndex(update.getKey(), Collections.emptyList(), Collections.singletonList(update.getEntry()));
        } finally {
            try{
                transaction.commit();
            } finally {
                tx.commit();
            }
        }
    }

    private static void forceRemoveElementFromMixedGraphIndex(Object elementIdToRemoveFromIndex,
                                                              IndexRecordEntry[] indexRecord,
                                                              StandardJanusGraph graph,
                                                              JanusGraphIndex index,
                                                              ManagementSystem managementSystem) throws BackendException {

        verifyIndexIsMixed(index);

        JanusGraphSchemaVertex indexSchemaVertex = managementSystem.getSchemaVertex(index);
        MixedIndexType indexType = (MixedIndexType) indexSchemaVertex.asIndexType();
        String elementKey = IndexRecordUtil.element2String(elementIdToRemoveFromIndex);

        StandardJanusGraphTx tx = (StandardJanusGraphTx) graph.newTransaction();
        BackendTransaction transaction = tx.getTxHandle();
        IndexTransaction indexTransaction = transaction.getIndexTransaction(indexType.getBackingIndexName());

        try{
            for(IndexRecordEntry indexRecordEntry : indexRecord){
                indexTransaction.delete(indexType.getStoreName(), elementKey,
                    IndexRecordUtil.key2Field(indexType, indexRecordEntry.getKey()),
                    indexRecordEntry.getValue(), false);
            }
        } finally {
            try{
                transaction.commit();
            } finally {
                tx.commit();
            }
        }
    }

    private static void forceRemoveElementFullyFromMixedGraphIndex(Object elementIdToRemoveFromIndex,
                                                                   StandardJanusGraph graph,
                                                                   JanusGraphIndex index,
                                                                   ManagementSystem managementSystem) throws BackendException {

        verifyIndexIsMixed(index);

        JanusGraphSchemaVertex indexSchemaVertex = managementSystem.getSchemaVertex(index);
        MixedIndexType indexType = (MixedIndexType) indexSchemaVertex.asIndexType();
        String elementKey = IndexRecordUtil.element2String(elementIdToRemoveFromIndex);

        StandardJanusGraphTx tx = (StandardJanusGraphTx) graph.newTransaction();
        BackendTransaction transaction = tx.getTxHandle();
        IndexTransaction indexTransaction = transaction.getIndexTransaction(indexType.getBackingIndexName());

        try{
            indexTransaction.delete(indexType.getStoreName(), elementKey, ANY_UPDATE_KEY, ANY_UPDATE_VALUE, true);
        } finally {
            try{
                transaction.commit();
            } finally {
                tx.commit();
            }
        }
    }

    private static IndexRecordEntry[] toCompositeIndexRecord(PropertyKey[] propertyKeys, Map<String, Object> indexRecordPropertyValues){

        if(indexRecordPropertyValues.size() != propertyKeys.length){
            throw new IllegalArgumentException("indexRecordPropertyValues contains "+indexRecordPropertyValues.size()
                +" properties but provided index has "+propertyKeys.length+" indexed properties. " +
                "It is necessary to include all but only indexed properties in indexRecordPropertyValues.");
        }

        IndexRecordEntry[] indexRecord = new IndexRecordEntry[propertyKeys.length];

        for(int i=0; i<propertyKeys.length; i++){
            PropertyKey propertyKey = propertyKeys[i];
            String propertyKeyName = propertyKey.name();
            if(!indexRecordPropertyValues.containsKey(propertyKeyName)){
                throw new IllegalArgumentException("indexRecordPropertyValues doesn't contain property "+propertyKeyName
                    +" but provided index has this property. It is necessary to include all but only indexed properties in indexRecordPropertyValues.");
            }
            Object propertyValue = indexRecordPropertyValues.get(propertyKeyName);
            long propertyKeyId = propertyKey.longId();
            indexRecord[i] = new IndexRecordEntry(propertyKeyId, propertyValue, propertyKey);
        }

        return indexRecord;
    }

    private static IndexRecordEntry[] toMixedIndexRecord(PropertyKey[] propertyKeys, Map<String, Object> indexRecordPropertyValues){

        Map<String, PropertyKey> propertyKeyMap = new HashMap<>(propertyKeys.length);
        for(PropertyKey propertyKey : propertyKeys){
            propertyKeyMap.put(propertyKey.name(), propertyKey);
        }

        IndexRecordEntry[] indexRecord = new IndexRecordEntry[indexRecordPropertyValues.size()];
        Iterator<Map.Entry<String, Object>> recordPropertyIt = indexRecordPropertyValues.entrySet().iterator();

        for(int i=0; i<indexRecord.length; i++){
            Map.Entry<String, Object> recordProperty = recordPropertyIt.next();
            String propertyName = recordProperty.getKey();
            PropertyKey propertyKey = propertyKeyMap.get(propertyName);
            if(propertyKey == null){
                throw new IllegalArgumentException("indexRecordPropertyValues contains property "+propertyName
                    +" which isn't presented in the index.");
            }
            long propertyKeyId = propertyKey.longId();
            indexRecord[i] = new IndexRecordEntry(propertyKeyId, recordProperty.getValue(), propertyKey);
        }

        return indexRecord;
    }

    private static void verifyIndexIsComposite(JanusGraphIndex index){
        if(!index.isCompositeIndex()){
            throw new IllegalArgumentException("Index ["+index.name()+"] is not a Composite index but a Composite index is expected");
        }
    }

    private static void verifyIndexIsMixed(JanusGraphIndex index){
        if(!index.isMixedIndex()){
            throw new IllegalArgumentException("Index ["+index.name()+"] is not a Mixed index but a Mixed index is expected");
        }
    }
}
