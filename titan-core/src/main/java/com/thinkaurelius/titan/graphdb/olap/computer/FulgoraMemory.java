package com.thinkaurelius.titan.graphdb.olap.computer;

import org.apache.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.tinkerpop.gremlin.tinkergraph.process.computer.TinkerMemory;

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
