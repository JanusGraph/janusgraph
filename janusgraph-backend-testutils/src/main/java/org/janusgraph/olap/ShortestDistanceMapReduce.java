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

package org.janusgraph.olap;

import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.KeyValue;
import org.apache.tinkerpop.gremlin.process.computer.util.StaticMapReduce;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.Iterator;

public class ShortestDistanceMapReduce extends StaticMapReduce<Object, Long, Object, Long, Iterator<KeyValue<Object, Long>>> {

    public static final String SHORTEST_DISTANCE_MEMORY_KEY = "janusgraph.shortestDistanceMapReduce.memoryKey";
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
    public void loadState(final Graph graph, final Configuration configuration) {
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
