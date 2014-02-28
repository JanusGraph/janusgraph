package com.thinkaurelius.titan.graphdb.fulgora;

import com.thinkaurelius.titan.graphdb.types.system.EmptyVertex;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class FulgoraNeighborVertex extends EmptyVertex {

    private final long id;
    private final FulgoraExecutor executor;

    public FulgoraNeighborVertex(long id, FulgoraExecutor executor) {
        this.id = id;
        this.executor = executor;
    }

    @Override
    public<A> A getProperty(String key) {
        if (key.equals(executor.stateKey)) {
            return (A)executor.getVertexState(getID());
        } else return super.getProperty(key);
    }

    @Override
    public long getID() {
        return id;
    }

    @Override
    public boolean hasId() {
        return true;
    }

}
