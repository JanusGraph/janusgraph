// Copyright 2017 JanusGraph Authors
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

package org.janusgraph.graphdb.olap.computer;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import org.apache.tinkerpop.gremlin.process.computer.MessageCombiner;
import org.apache.tinkerpop.gremlin.process.computer.MessageScope;
import org.apache.tinkerpop.gremlin.process.computer.VertexComputeKey;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import org.cliffc.high_scale_lib.NonBlockingHashMapLong;
import org.janusgraph.diskstorage.EntryList;
import org.janusgraph.graphdb.idmanagement.IDManager;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class FulgoraVertexMemory<M> {

    private static final MessageScope.Global GLOBAL_SCOPE = MessageScope.Global.instance();



    private final NonBlockingHashMapLong<VertexState<M>> vertexStates;
    private final IDManager idManager;
    private final Set<VertexComputeKey> computeKeys;
    private final Map<String,Integer> elementKeyMap;
    private final MessageCombiner<M> combiner;
    private Map<MessageScope,Integer> previousScopes;
    private Map<MessageScope,Integer> currentScopes;
    private boolean inExecute;

    private final NonBlockingHashMapLong<PartitionVertexAggregate<M>> partitionVertices;

    public FulgoraVertexMemory(int numVertices, final IDManager idManager, final VertexProgram<M> vertexProgram) {
        Preconditions.checkArgument(numVertices>=0 && vertexProgram!=null && idManager!=null);
        vertexStates = new NonBlockingHashMapLong<>(numVertices);
        partitionVertices = new NonBlockingHashMapLong<>(64);
        this.idManager = idManager;
        this.combiner = vertexProgram.getMessageCombiner().orElse(null);
        this.computeKeys = vertexProgram.getVertexComputeKeys();
        this.elementKeyMap = getIdMap(vertexProgram.getVertexComputeKeys().stream().map(VertexComputeKey::getKey).collect(Collectors.toCollection(HashSet::new)));
        this.previousScopes = Collections.emptyMap();
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

    public Set<MessageScope> getPreviousScopes() {
        return previousScopes.keySet();
    }

    public<V> void setProperty(long vertexId, String key, V value) {
        get(vertexId,true).setProperty(key,value,elementKeyMap);
    }

    public<V> V getProperty(long vertexId, String key) {
        return get(vertexId,false).getProperty(key,elementKeyMap);
    }

    void sendMessage(long vertexId, M message, MessageScope scope) {
        VertexState<M> state = get(vertexId,true);
        if (scope instanceof MessageScope.Global) state.addMessage(message,GLOBAL_SCOPE,currentScopes,combiner);
        else state.setMessage(message,scope,currentScopes);
    }

    Stream<M> getMessage(long vertexId, MessageScope scope) {
        return get(vertexId,false).getMessage(normalizeScope(scope),previousScopes);
    }

    void completeIteration() {
        for (VertexState<M> state : vertexStates.values()) state.completeIteration();
        partitionVertices.clear();
        previousScopes = currentScopes;
        inExecute = false;
    }

    void nextIteration(Set<MessageScope> scopes) {
        currentScopes = getIdMap(normalizeScopes(scopes));
        partitionVertices.clear();
        inExecute = true;
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

    public Set<String> getMemoryKeys() {
        return computeKeys.stream().filter(key -> inExecute || !key.isTransient()).map(VertexComputeKey::getKey).collect(Collectors.toSet());
    }

    private static MessageScope normalizeScope(MessageScope scope) {
        if (scope instanceof MessageScope.Global) return GLOBAL_SCOPE;
        else return scope;
    }

    private static Iterable<MessageScope> normalizeScopes(Iterable<MessageScope> scopes) {
        return Iterables.transform(scopes, FulgoraVertexMemory::normalizeScope);
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
        getPartitioned(vertexId).addMessage(message,normalizeScope(scope),previousScopes,combiner);
    }

    Stream<M> getAggregateMessage(long vertexId, MessageScope scope) {
        return getPartitioned(vertexId).getMessage(normalizeScope(scope),previousScopes);
    }

    public Map<Long,EntryList> retrievePartitionAggregates() {
        for (PartitionVertexAggregate agg : partitionVertices.values()) agg.completeIteration();
        return Maps.transformValues(partitionVertices, PartitionVertexAggregate::getLoadedProperties);
    }

    public static <K> Map<K,Integer> getIdMap(Iterable<K> elements) {
        Map<K,Integer> b = new HashMap<>();
        int size = 0;
        for (K key : elements) {
            b.put(key,size++);
        }
        return Collections.unmodifiableMap(b);
    }


}
