package com.thinkaurelius.titan.graphdb.olap.computer;

import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.process.computer.VertexProgram;
import com.tinkerpop.gremlin.tinkergraph.process.computer.TinkerMemory;

import java.util.Set;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class FulgoraMemory extends TinkerMemory {

    public FulgoraMemory(VertexProgram<?> vertexProgram, Set<MapReduce> mapReducers) {
        super(vertexProgram, mapReducers);
    }


    protected void completeSubRound() {
        super.completeSubRound();
    }

    protected void complete() {
        super.complete();
    }

}
