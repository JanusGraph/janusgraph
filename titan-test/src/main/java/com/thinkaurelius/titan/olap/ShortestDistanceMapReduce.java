package com.thinkaurelius.titan.olap;

import com.tinkerpop.gremlin.process.computer.KeyValue;
import com.tinkerpop.gremlin.process.computer.util.StaticMapReduce;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.commons.configuration.Configuration;

import java.util.Iterator;

public class ShortestDistanceMapReduce extends StaticMapReduce<Object, Long, Object, Long, Iterator<KeyValue<Object, Long>>> {

    public static final String SHORTEST_DISTANCE_MEMORY_KEY = "titan.shortestDistanceMapReduce.memoryKey";
    public static final String DEFAULT_MEMORY_KEY = "shortestDistance";

    private String memoryKey = DEFAULT_MEMORY_KEY;

    private ShortestDistanceMapReduce() {

    }

    private ShortestDistanceMapReduce(final String memoryKey) {
        this.memoryKey = memoryKey;
    }

    @Override
    public void storeState(final Configuration configuration) {
        super.storeState(configuration);
        configuration.setProperty(SHORTEST_DISTANCE_MEMORY_KEY, this.memoryKey);
    }

    @Override
    public void loadState(final Configuration configuration) {
        this.memoryKey = configuration.getString(SHORTEST_DISTANCE_MEMORY_KEY, DEFAULT_MEMORY_KEY);
    }

    @Override
    public boolean doStage(final Stage stage) {
        return stage.equals(Stage.MAP);
    }

    @Override
    public void map(final Vertex vertex, final MapEmitter<Object, Long> emitter) {
        final Property distance = vertex.property(ShortestDistanceVertexProgram.DISTANCE);
        if (distance.isPresent()) {
            emitter.emit(vertex.id(), (Long) distance.value());
        }
    }

    @Override
    public Iterator<KeyValue<Object, Long>> generateFinalResult(final Iterator<KeyValue<Object, Long>> keyValues) {
        return keyValues;
    }

    @Override
    public String getMemoryKey() {
        return this.memoryKey;
    }

    @Override
    public String toString() {
        return StringFactory.mapReduceString(this, this.memoryKey);
    }

    //////////////////////////////

    public static Builder build() {
        return new Builder();
    }

    public static class Builder {

        private String memoryKey = DEFAULT_MEMORY_KEY;

        private Builder() {

        }

        public Builder memoryKey(final String memoryKey) {
            this.memoryKey = memoryKey;
            return this;
        }

        public ShortestDistanceMapReduce create() {
            return new ShortestDistanceMapReduce(this.memoryKey);
        }

    }

}
