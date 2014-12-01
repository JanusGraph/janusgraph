package com.thinkaurelius.titan.graphdb.olap.computer;

import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.tinkergraph.process.computer.TinkerReduceEmitter;
import com.tinkerpop.gremlin.process.computer.KeyValue;

import java.util.Queue;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class FulgoraReduceEmitter<OK,OV> extends TinkerReduceEmitter<OK, OV> {

    protected void complete(final MapReduce<?, ?, OK, OV, ?> mapReduce) {
        super.complete(mapReduce);
    }

    protected Queue<KeyValue<OK, OV>> getQueue() {
        return reduceQueue;
    }

}
