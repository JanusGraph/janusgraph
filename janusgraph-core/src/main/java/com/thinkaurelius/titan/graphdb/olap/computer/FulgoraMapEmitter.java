package com.thinkaurelius.titan.graphdb.olap.computer;

import org.apache.tinkerpop.gremlin.process.computer.KeyValue;
import org.apache.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.tinkerpop.gremlin.tinkergraph.process.computer.TinkerMapEmitter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class FulgoraMapEmitter<K, V> implements MapReduce.MapEmitter<K, V> {

    public Map<K, Queue<V>> reduceMap;
    public Queue<KeyValue<K, V>> mapQueue;
    private final boolean doReduce;

    public FulgoraMapEmitter(final boolean doReduce) {
        this.doReduce = doReduce;
        if (this.doReduce)
            this.reduceMap = new ConcurrentHashMap<>();
        else
            this.mapQueue = new ConcurrentLinkedQueue<>();
    }

    @Override
    public void emit(K key, V value) {
        if (this.doReduce)
            this.reduceMap.computeIfAbsent(key, k -> new ConcurrentLinkedQueue<>()).add(value);
        else
            this.mapQueue.add(new KeyValue<>(key, value));
    }

    protected void complete(final MapReduce<K, V, ?, ?, ?> mapReduce) {
        if (!this.doReduce && mapReduce.getMapKeySort().isPresent()) {
            final Comparator<K> comparator = mapReduce.getMapKeySort().get();
            final List<KeyValue<K, V>> list = new ArrayList<>(this.mapQueue);
            Collections.sort(list, Comparator.comparing(KeyValue::getKey, comparator));
            this.mapQueue.clear();
            this.mapQueue.addAll(list);
        } else if (mapReduce.getMapKeySort().isPresent()) {
            final Comparator<K> comparator = mapReduce.getMapKeySort().get();
            final List<Map.Entry<K, Queue<V>>> list = new ArrayList<>();
            list.addAll(this.reduceMap.entrySet());
            Collections.sort(list, Comparator.comparing(Map.Entry::getKey, comparator));
            this.reduceMap = new LinkedHashMap<>();
            list.forEach(entry -> this.reduceMap.put(entry.getKey(), entry.getValue()));
        }
    }
}
