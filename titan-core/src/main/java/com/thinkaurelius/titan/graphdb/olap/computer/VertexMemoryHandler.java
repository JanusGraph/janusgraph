package com.thinkaurelius.titan.graphdb.olap.computer;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.TitanEdge;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.core.TitanVertexProperty;
import com.thinkaurelius.titan.graphdb.vertices.PreloadedVertex;
import org.apache.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.computer.MessageScope;
import org.apache.tinkerpop.gremlin.process.computer.Messenger;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.*;
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

    <V> TitanVertexProperty<V> constructProperty(String key, V value) {
        assert key!=null && value!=null;
        return new FulgoraVertexProperty<V>(this,vertex,key,value);
    }

    @Override
    public <V> Iterator<VertexProperty<V>> properties(String... keys) {
        final Set<String> memoryKeys = vertexMemory.getMemoryKeys();
        if (memoryKeys.isEmpty()) return Collections.emptyIterator();
        if (keys==null || keys.length==0) {
            keys = memoryKeys.stream().filter(k -> !k.equals(TraversalVertexProgram.HALTED_TRAVERSERS)).toArray(String[]::new);
        }
        List<VertexProperty<V>> result = new ArrayList<>(Math.min(keys.length,memoryKeys.size()));
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
    public <V> TitanVertexProperty<V> property(VertexProperty.Cardinality cardinality, String key, V value) {
        if (!supports(key)) throw GraphComputer.Exceptions.providedKeyIsNotAnElementComputeKey(key);
        Preconditions.checkArgument(value != null);
        Preconditions.checkArgument(cardinality== VertexProperty.Cardinality.single,"Only single cardinality is supported, provided: %s",cardinality);
        vertexMemory.setProperty(vertexId, key, value);
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
            M message = vertexMemory.getMessage(vertexId,messageScope);
            if (message == null) return Stream.empty();
            else return Stream.of(message);
        } else {
            final MessageScope.Local<M> localMessageScope = (MessageScope.Local) messageScope;
            final Traversal<Vertex, Edge> reverseIncident = FulgoraUtil.getReverseElementTraversal(localMessageScope,vertex,vertex.tx());
            final BiFunction<M,Edge,M> edgeFct = localMessageScope.getEdgeFunction();

            return IteratorUtils.stream(reverseIncident)
                    .map(e -> {
                        M msg = vertexMemory.getMessage(vertexMemory.getCanonicalId(((TitanEdge) e).otherVertex(vertex).longId()), localMessageScope);
                        return msg == null ? null : edgeFct.apply(msg, e);
                    })
                    .filter(m -> m != null);
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
        public Stream<M> receiveMessages(MessageScope messageScope) {
            if (messageScope instanceof MessageScope.Global) {
                return super.receiveMessages(messageScope);
            } else {
                final MessageScope.Local<M> localMessageScope = (MessageScope.Local) messageScope;
                M aggregateMsg = vertexMemory.getAggregateMessage(vertexId,localMessageScope);
                if (aggregateMsg==null) return Stream.empty();
                else return Stream.of(aggregateMsg);
            }
        }

    }

}
