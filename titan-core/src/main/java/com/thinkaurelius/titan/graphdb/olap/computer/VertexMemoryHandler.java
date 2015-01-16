package com.thinkaurelius.titan.graphdb.olap.computer;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.thinkaurelius.titan.core.TitanEdge;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.core.TitanVertexProperty;
import com.thinkaurelius.titan.graphdb.vertices.PreloadedVertex;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.process.computer.MessageScope;
import com.tinkerpop.gremlin.process.computer.Messenger;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.VertexProperty;
import com.tinkerpop.gremlin.util.StreamFactory;

import java.util.*;
import java.util.function.BiFunction;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
class VertexMemoryHandler<M> implements PreloadedVertex.PropertyMixing, Messenger<M> {

    protected final FulgoraVertexMemory<M> vertexMemory;
    private final PreloadedVertex vertex;
    protected final long vertexId;

    VertexMemoryHandler(FulgoraVertexMemory<M> vertexMemory, PreloadedVertex vertex) {
        assert vertex!=null && vertexMemory!=null;
        this.vertexMemory = vertexMemory;
        this.vertex = vertex;
        this.vertexId = vertexMemory.getCanonicalId(vertex.longId());
    }

    void removeKey(String key) {
        vertexMemory.setProperty(vertexId,key,null);
    }

    <V> TitanVertexProperty<V> constructProperty(String key, V value) {
        assert key!=null && value!=null;
        return new FulgoraVertexProperty<V>(this,vertex,key,value);
    }

    @Override
    public <V> Iterator<VertexProperty<V>> propertyIterator(String... keys) {
        if (vertexMemory.elementKeyMap.isEmpty()) return Collections.emptyIterator();
        if (keys==null || keys.length==0) keys = vertexMemory.elementKeyMap.keySet()
                .toArray(new String[vertexMemory.elementKeyMap.size()]);
        List<VertexProperty<V>> result = new ArrayList<>(Math.min(keys.length,vertexMemory.elementKeyMap.size()));
        for (String key : keys) {
            if (!supports(key)) continue;
            V value = vertexMemory.getProperty(vertexId,key);
            if (value!=null) result.add(constructProperty(key,value));
        }
        return result.iterator();
    }

    @Override
    public boolean supports(String key) {
        return vertexMemory.elementKeyMap.containsKey(key);
    }

    @Override
    public <V> TitanVertexProperty<V> property(String key, V value) {
        return singleProperty(key,value);
    }

    @Override
    public <V> TitanVertexProperty<V> singleProperty(String key, V value) {
        if (!supports(key)) throw GraphComputer.Exceptions.providedKeyIsNotAnElementComputeKey(key);
        Preconditions.checkArgument(value != null);
        vertexMemory.setProperty(vertexId, key, value);
        return constructProperty(key,value);
    }

    @Override
    public Iterable<M> receiveMessages(MessageScope messageScope) {
        if (messageScope instanceof MessageScope.Global) {
            M message = vertexMemory.getMessage(vertexId,messageScope);
            if (message == null) return Collections.EMPTY_LIST;
            else return ImmutableList.of(message);
        } else {
            final MessageScope.Local<M> localMessageScope = (MessageScope.Local) messageScope;
            final Traversal<Vertex, Edge> reverseIncident = FulgoraUtil.getReverseElementTraversal(localMessageScope,vertex,vertex.tx());
            final BiFunction<M,Edge,M> edgeFct = localMessageScope.getEdgeFunction();

            return StreamFactory.iterable(StreamFactory.stream(reverseIncident)
                    .map(e -> {
                        M msg = vertexMemory.getMessage(((TitanEdge) e).otherVertex(vertex).longId(), localMessageScope);
                        return msg == null ? null : edgeFct.apply(msg, e);
                    })
                    .filter(m -> m != null)
            );
        }
    }

    @Override
    public void sendMessage(MessageScope messageScope, M m) {
        if (messageScope instanceof MessageScope.Local) {
            vertexMemory.sendMessage(vertexId, m, messageScope);
        } else {
            ((MessageScope.Global) messageScope).vertices().forEach(v -> {
                long vertexId;
                if (v instanceof TitanVertex) vertexId=((TitanVertex)v).longId();
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
        public Iterable<M> receiveMessages(MessageScope messageScope) {
            if (messageScope instanceof MessageScope.Global) {
                return super.receiveMessages(messageScope);
            } else {
                final MessageScope.Local<M> localMessageScope = (MessageScope.Local) messageScope;
                M aggregateMsg = vertexMemory.getAggregateMessage(vertexId,localMessageScope);
                if (aggregateMsg==null) return Collections.EMPTY_LIST;
                else return ImmutableList.of(aggregateMsg);
            }
        }

    }

}
