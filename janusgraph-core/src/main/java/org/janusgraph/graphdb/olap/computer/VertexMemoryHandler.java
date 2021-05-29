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

package org.janusgraph.graphdb.olap.computer;

import com.google.common.base.Preconditions;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.computer.MessageScope;
import org.apache.tinkerpop.gremlin.process.computer.Messenger;
import org.apache.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.janusgraph.core.JanusGraphEdge;
import org.janusgraph.core.JanusGraphException;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.core.JanusGraphVertexProperty;
import org.janusgraph.graphdb.vertices.PreloadedVertex;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Stream;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
class VertexMemoryHandler<M> implements PreloadedVertex.PropertyMixing, Messenger<M> {

    protected final FulgoraVertexMemory<M> vertexMemory;
    private final PreloadedVertex vertex;
    protected final long vertexId;
    private boolean inExecute;

    VertexMemoryHandler(FulgoraVertexMemory<M> vertexMemory, PreloadedVertex vertex) {
        assert vertex!=null && vertexMemory!=null;
        this.vertexMemory = vertexMemory;
        this.vertex = vertex;
        this.vertexId = vertexMemory.getCanonicalId(vertex.longId());
        this.inExecute = false;
    }

    void removeKey(String key) {
        vertexMemory.setProperty(vertexId,key,null);
    }

    <V> JanusGraphVertexProperty<V> constructProperty(String key, V value) {
        assert key!=null && value!=null;
        return new FulgoraVertexProperty<>(this, vertex, key, value);
    }

    @Override
    public <V> Iterator<VertexProperty<V>> properties(String... keys) {
        final Set<String> memoryKeys = vertexMemory.getMemoryKeys();
        if (memoryKeys.isEmpty()) return Collections.emptyIterator();
        if (keys==null || keys.length==0) {
            keys = memoryKeys.stream().filter(k -> !k.equals(TraversalVertexProgram.HALTED_TRAVERSERS)).toArray(String[]::new);
        }
        final List<VertexProperty<V>> result = new ArrayList<>(Math.min(keys.length,memoryKeys.size()));
        for (String key : keys) {
            if (!supports(key)) continue;
            V value = vertexMemory.getProperty(vertexId,key);
            if (value!=null) result.add(constructProperty(key,value));
        }
        return result.iterator();
    }

    @Override
    public boolean supports(String key) {
        return vertexMemory.getMemoryKeys().contains(key);
    }

    @Override
    public <V> JanusGraphVertexProperty<V> property(VertexProperty.Cardinality cardinality, String key, V value) {
        if (!supports(key)) throw GraphComputer.Exceptions.providedKeyIsNotAnElementComputeKey(key);
        Preconditions.checkNotNull(value);
        if (cardinality == VertexProperty.Cardinality.single) {
            vertexMemory.setProperty(vertexId, key, value);
        } else {
            final V previousValue = vertexMemory.getProperty(vertexId, key);
            final List<V> values;
            if (previousValue != null) {
                assert previousValue instanceof List;
                values = new ArrayList<>(((List) previousValue));
            } else {
                values = new ArrayList<>();
            }
            values.add(value);
            vertexMemory.setProperty(vertexId, key, values);
        }
        return constructProperty(key,value);
    }

    public boolean isInExecute() {
        return inExecute;
    }

    public void setInExecute(boolean inExecute) {
        this.inExecute = inExecute;
    }

    public Stream<M> receiveMessages(MessageScope messageScope) {
        if (messageScope instanceof MessageScope.Global) {
            return vertexMemory.getMessage(vertexId, messageScope);
        } else {
            final MessageScope.Local<M> localMessageScope = (MessageScope.Local) messageScope;
            final BiFunction<M,Edge,M> edgeFct = localMessageScope.getEdgeFunction();
            final List<Edge> edges;
            try (final Traversal<Vertex, Edge> reverseIncident = FulgoraUtil.getReverseElementTraversal(localMessageScope,vertex,vertex.tx())) {
                edges = IteratorUtils.list(reverseIncident);
            } catch (Exception e) {
                throw new JanusGraphException("Unable to close traversal", e);
            }

            return edges.stream()
                        .flatMap(e -> {
                            long canonicalId = vertexMemory.getCanonicalId(((JanusGraphEdge) e).otherVertex(vertex).longId());
                            return vertexMemory.getMessage(canonicalId, localMessageScope)
                                               .map(msg -> msg == null ? null : edgeFct.apply(msg, e));
                        })
                        .filter(Objects::nonNull);
        }
    }

    @Override
    public Iterator<M> receiveMessages() {
        Stream<M> combinedStream = Stream.empty();
        for (MessageScope scope : vertexMemory.getPreviousScopes()) {
            combinedStream = Stream.concat(receiveMessages(scope),combinedStream);
        }
        return combinedStream.iterator();
    }

    @Override
    public void sendMessage(MessageScope messageScope, M m) {
        if (messageScope instanceof MessageScope.Local) {
            vertexMemory.sendMessage(vertexId, m, messageScope);
        } else {
            ((MessageScope.Global) messageScope).vertices().forEach(v -> {
                long vertexId;
                if (v instanceof JanusGraphVertex) vertexId=((JanusGraphVertex)v).longId();
                else vertexId = (Long)v.id();
                vertexMemory.sendMessage(vertexMemory.getCanonicalId(vertexId), m, messageScope);
            });
        }
    }

    static class Partition<M> extends VertexMemoryHandler<M> {

        Partition(FulgoraVertexMemory<M> vertexMemory, PreloadedVertex vertex) {
            super(vertexMemory, vertex);
        }

        @Override
        public Stream<M> receiveMessages(MessageScope messageScope) {
            if (messageScope instanceof MessageScope.Global) {
                return super.receiveMessages(messageScope);
            } else {
                final MessageScope.Local<M> localMessageScope = (MessageScope.Local) messageScope;
                return vertexMemory.getAggregateMessage(vertexId,localMessageScope);
            }
        }
    }
}
