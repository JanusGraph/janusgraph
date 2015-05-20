package com.thinkaurelius.titan.graphdb.olap.computer;

import org.apache.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.tinkerpop.gremlin.tinkergraph.process.computer.TinkerReduceEmitter;
import org.apache.tinkerpop.gremlin.process.computer.KeyValue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class FulgoraReduceEmitter<OK, OV> implements MapReduce.ReduceEmitter<OK, OV> {

    protected Queue<KeyValue<OK, OV>> reduceQueue = new ConcurrentLinkedQueue<>();

    @Override
    public void emit(final OK key, final OV value) {
        this.reduceQueue.add(new KeyValue<>(key, value));
    }

    protected void complete(final MapReduce<?, ?, OK, OV, ?> mapReduce) {
        if (mapReduce.getReduceKeySort().isPresent()) {
            final Comparator<OK> comparator = mapReduce.getReduceKeySort().get();
            final List<KeyValue<OK, OV>> list = new ArrayList<>(this.reduceQueue);
            Collections.sort(list, Comparator.comparing(KeyValue::getKey, comparator));
            this.reduceQueue.clear();
            this.reduceQueue.addAll(list);
        }
    }
}
