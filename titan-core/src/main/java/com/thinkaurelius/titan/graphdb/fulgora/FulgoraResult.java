package com.thinkaurelius.titan.graphdb.fulgora;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.olap.OLAPResult;
import com.thinkaurelius.titan.core.olap.State;
import com.thinkaurelius.titan.graphdb.idmanagement.IDManager;
import org.cliffc.high_scale_lib.NonBlockingHashMapLong;

import java.util.Map;
import java.util.concurrent.ConcurrentMap;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class FulgoraResult<S extends State<S>> implements OLAPResult<S> {


    private NonBlockingHashMapLong<S> vertexStates;
    private final IDManager idManager;

    public FulgoraResult(int numVertices, final IDManager idManager) {
        Preconditions.checkArgument(numVertices>=0);
        vertexStates = new NonBlockingHashMapLong<S>(numVertices);
        this.idManager = idManager;
    }

    public FulgoraResult(Map<Long,S> initialState, final IDManager idManager) {
        this(Math.max(initialState.size(), 0),idManager);
        for (Map.Entry<Long,S> entry : initialState.entrySet()) {
            long vertexid = entry.getKey();
            if (idManager.isPartitionedVertex(vertexid)) {
                S state = entry.getValue();
                for (long repId : idManager.getPartitionedVertexRepresentatives(vertexid)) {
                    vertexStates.put(repId,state.clone());
                }
            } else {
                vertexStates.put(vertexid,entry.getValue());
            }
        }
    }

    void set(long vertexId, S state) {
        vertexStates.put(vertexId,state);
    }

    void mergePartitionedVertexStates() {
        for (long vertexid : vertexStates.keySet()) {
            if (idManager.isPartitionedVertex(vertexid) && vertexid==idManager.getCanonicalVertexId(vertexid)) {
                S mergedState = null;
                for (long pid : idManager.getPartitionedVertexRepresentatives(vertexid)) {
                    S state = vertexStates.get(pid);
                    assert state!=null;
                    if (mergedState==null) mergedState = state.clone();
                    else mergedState.merge(state);
                    vertexStates.put(pid,mergedState);
                }
            }
        }
    }

    @Override
    public Iterable<S> values() {
        return vertexStates.values();
    }

    @Override
    public Iterable<Map.Entry<Long, S>> entries() {
        return vertexStates.entrySet();
    }

    @Override
    public long size() {
        return vertexStates.size();
    }

    @Override
    public S get(long vertexid) {
        return vertexStates.get(vertexid);
    }
}
