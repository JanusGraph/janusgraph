package com.thinkaurelius.titan.graphdb.olap.computer;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.thinkaurelius.titan.core.TitanVertexProperty;
import com.thinkaurelius.titan.diskstorage.EntryList;
import com.thinkaurelius.titan.graphdb.idmanagement.IDManager;
import com.thinkaurelius.titan.graphdb.vertices.PreloadedVertex;
import com.tinkerpop.gremlin.process.computer.MessageCombiner;
import com.tinkerpop.gremlin.process.computer.MessageScope;
import com.tinkerpop.gremlin.process.computer.Messenger;
import com.tinkerpop.gremlin.process.computer.VertexProgram;
import com.tinkerpop.gremlin.structure.VertexProperty;
import org.cliffc.high_scale_lib.NonBlockingHashMapLong;

import java.util.*;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class FulgoraVertexMemory<M> {

    private NonBlockingHashMapLong<VertexState<M>> vertexStates;
    private final IDManager idManager;
    final Map<String,Integer> elementKeyMap;
    private final MessageCombiner<M> combiner;
    private Map<MessageScope,Integer> previousScopes;
    private Map<MessageScope,Integer> currentScopes;

    private NonBlockingHashMapLong<PartitionVertexAggregate<M>> partitionVertices;

    public FulgoraVertexMemory(int numVertices, final IDManager idManager, final VertexProgram<M> vertexProgram) {
        Preconditions.checkArgument(numVertices>=0 && vertexProgram!=null && idManager!=null);
        Preconditions.checkArgument(vertexProgram.getMessageCombiner().isPresent(),"A message combiner needs to be provided to conserve memory.");
        vertexStates = new NonBlockingHashMapLong<>(numVertices);
        partitionVertices = new NonBlockingHashMapLong<>(64);
        this.idManager = idManager;
        this.combiner = vertexProgram.getMessageCombiner().get();
        this.elementKeyMap = getIdMap(vertexProgram.getElementComputeKeys());
        this.previousScopes = ImmutableMap.of();
    }

    private VertexState<M> get(long vertexId, boolean create) {
        assert vertexId==getCanonicalId(vertexId);
        VertexState<M> state = vertexStates.get(vertexId);
        if (state==null) {
            if (!create) return VertexState.EMPTY_STATE;
            vertexStates.putIfAbsent(vertexId,new VertexState<>(elementKeyMap));
            state = vertexStates.get(vertexId);
        }
        return state;
    }

    public long getCanonicalId(long vertexId) {
        if (!idManager.isPartitionedVertex(vertexId)) return vertexId;
        else return idManager.getCanonicalVertexId(vertexId);
    }

    public<V> void setProperty(long vertexId, String key, V value) {
        get(vertexId,true).setProperty(key,value,elementKeyMap);
    }

    public<V> V getProperty(long vertexId, String key) {
        return get(vertexId,false).getProperty(key,elementKeyMap);
    }

    void setMessage(long vertexId, M message, MessageScope scope) {
        VertexState<M> state = get(vertexId,true);
        if (scope instanceof MessageScope.Global) state.addMessage(message,scope,currentScopes,combiner);
        else state.setMessage(message,scope,currentScopes);
    }

    M getMessage(long vertexId, MessageScope scope) {
        return get(vertexId,false).getMessage(scope,previousScopes);
    }

    void completeIteration() {
        for (VertexState<M> state : vertexStates.values()) state.completeIteration();
        partitionVertices.clear();
        previousScopes = currentScopes;
    }

    void nextIteration(Set<MessageScope> scopes) {
        currentScopes = getIdMap(scopes);
        partitionVertices.clear();
    }

    public Map<Long,Map<String,Object>> getMutableVertexProperties() {
        return Maps.transformValues(vertexStates, vs -> {
            Map<String,Object> map = new HashMap<>(elementKeyMap.size());
            for (String key : elementKeyMap.keySet()) {
                Object v = vs.getProperty(key,elementKeyMap);
                if (v!=null) map.put(key,v);
            }
            return map;
        });
    }


    //######## Partitioned Vertices ##########

    private PartitionVertexAggregate<M> getPartitioned(long vertexId) {
        assert idManager.isPartitionedVertex(vertexId);
        vertexId=getCanonicalId(vertexId);
        PartitionVertexAggregate<M> state = partitionVertices.get(vertexId);
        if (state==null) {
            partitionVertices.putIfAbsent(vertexId,new PartitionVertexAggregate<>(previousScopes));
            state = partitionVertices.get(vertexId);
        }
        return state;
    }

    public void setLoadedProperties(long vertexId, EntryList entries) {
        getPartitioned(vertexId).setLoadedProperties(entries);
    }

    public void aggregateMessage(long vertexId, M message, MessageScope scope) {
        getPartitioned(vertexId).addMessage(message,scope,previousScopes,combiner);
    }

    public M getAggregateMessage(long vertexId, MessageScope scope) {
        return getPartitioned(vertexId).getMessage(scope,previousScopes);
    }

    public Map<Long,EntryList> retrievePartitionAggregates() {
        for (PartitionVertexAggregate agg : partitionVertices.values()) agg.completeIteration();
        return Maps.transformValues(partitionVertices, s-> s.getLoadedProperties());
    }

    public static <K> Map<K,Integer> getIdMap(Set<K> elements) {
        ImmutableMap.Builder<K,Integer> b = ImmutableMap.builder();
        int size = 0;
        for (K key : elements) {
            b.put(key,size++);
        }
        return b.build();
    }


}
