package com.thinkaurelius.titan.graphdb.fulgora;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.olap.OLAPResult;
import com.thinkaurelius.titan.graphdb.idmanagement.IDManager;
import org.cliffc.high_scale_lib.NonBlockingHashMapLong;

import java.util.Map;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class FulgoraResult<S> implements OLAPResult<S> {

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
            long vertexId = entry.getKey();
            Preconditions.checkArgument(!idManager.isPartitionedVertex(vertexId) || vertexId==idManager.getCanonicalVertexId(vertexId));
            vertexStates.put(vertexId,entry.getValue());
        }
    }

    void set(long vertexId, S state) {
        Preconditions.checkArgument(!idManager.isPartitionedVertex(vertexId) || vertexId==idManager.getCanonicalVertexId(vertexId));
        vertexStates.put(vertexId,state);
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
    public S get(long vertexId) {
        if (idManager.isPartitionedVertex(vertexId)) vertexId=idManager.getCanonicalVertexId(vertexId);
        return vertexStates.get(vertexId);
    }
}
