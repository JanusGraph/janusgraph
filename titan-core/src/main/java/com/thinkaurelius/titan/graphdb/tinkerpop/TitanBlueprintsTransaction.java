package com.thinkaurelius.titan.graphdb.tinkerpop;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.graphdb.database.serialize.AttributeUtil;
import com.thinkaurelius.titan.graphdb.relations.RelationIdentifier;
import com.thinkaurelius.titan.graphdb.tinkerpop.optimize.TitanGraphTraversal;
import com.thinkaurelius.titan.graphdb.tinkerpop.optimize.TitanTraversal;
import com.thinkaurelius.titan.graphdb.types.system.BaseVertexLabel;
import com.tinkerpop.gremlin.process.T;
import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.structure.*;
import com.tinkerpop.gremlin.structure.util.ElementHelper;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Blueprints specific implementation of {@link TitanTransaction}.
 * Provides utility methods that wrap Titan calls with Blueprints terminology.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public abstract class TitanBlueprintsTransaction implements TitanTransaction {

    private static final Logger log =
            LoggerFactory.getLogger(TitanBlueprintsTransaction.class);

    /**
     * Returns the graph that this transaction is based on
     * @return
     */
    protected abstract TitanBlueprintsGraph getGraph();

    @Override
    public Features features() {
        return getGraph().features();
    }

    @Override
    public Variables variables() {
        return getGraph().variables();
    }

    @Override
    public Configuration configuration() {
        return getGraph().configuration();
    }

    /**
     * Creates a new vertex in the graph with the given vertex id.
     * Note, that an exception is thrown if the vertex id is not a valid Titan vertex id or if a vertex with the given
     * id already exists. Only accepts long ids - all others are ignored.
     * <p/>
     * Custom id setting must be enabled via the configuration option {@link com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration#ALLOW_SETTING_VERTEX_ID}.
     * <p/>
     * Use {@link com.thinkaurelius.titan.core.util.TitanId#toVertexId(long)} to construct a valid Titan vertex id from a user id.
     *
     * @param keyValues key-value pairs of properties to characterize or attach to the vertex
     * @return New vertex
     */
    @Override
    public TitanVertex addVertex(Object... keyValues) {
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        if (ElementHelper.getIdValue(keyValues).isPresent()) throw Vertex.Exceptions.userSuppliedIdsNotSupported();
        Object labelValue = null;
        for (int i = 0; i < keyValues.length; i = i + 2) {
            if (keyValues[i].equals(T.label)) {
                labelValue = keyValues[i+1];
                Preconditions.checkArgument(labelValue instanceof VertexLabel || labelValue instanceof String,
                        "Expected a string or VertexLabel as the vertex label argument, but got: %s",labelValue);
                if (labelValue instanceof String) ElementHelper.validateLabel((String) labelValue);
            }
        }
        VertexLabel label = BaseVertexLabel.DEFAULT_VERTEXLABEL;
        if (labelValue!=null) {
            label = (labelValue instanceof VertexLabel)?(VertexLabel)labelValue:getOrCreateVertexLabel((String) labelValue);
        }

        final TitanVertex vertex = addVertex(null,label);
        ElementHelper.attachProperties(vertex, keyValues);
        return vertex;
    }

//    @Override
//    public TitanVertex v(final Object id) {
//        if (null == id) throw Graph.Exceptions.elementNotFound(Vertex.class, null);
//
//        long vertexId;
//        if (id instanceof TitanVertex) //allows vertices to be "re-attached" to the current transaction
//            vertexId = ((TitanVertex) id).longId();
//        else if (id instanceof Long) {
//            vertexId = (Long) id;
//        } else if (id instanceof Number) {
//            vertexId = ((Number) id).longValue();
//        } else {
//            try {
//                vertexId = Long.valueOf(id.toString()).longValue();
//            } catch (NumberFormatException e) {
//                throw Graph.Exceptions.elementNotFound(Vertex.class, null);
//            }
//        }
//        TitanVertex vertex = getVertex(vertexId);
//
//        if (null == vertex)
//            throw Graph.Exceptions.elementNotFound(Vertex.class, id);
//        else
//            return vertex;
//
//    }
//
//    @Override
//    public TitanEdge e(final Object id) {
//        if (null == id) throw Graph.Exceptions.elementNotFound(Edge.class, null);
//        RelationIdentifier rid = null;
//
//        try {
//            if (id instanceof TitanEdge) rid = (RelationIdentifier) ((TitanEdge) id).id();
//            else if (id instanceof RelationIdentifier) rid = (RelationIdentifier) id;
//            else if (id instanceof String) rid = RelationIdentifier.parse((String) id);
//            else if (id instanceof long[]) rid = RelationIdentifier.get((long[]) id);
//            else if (id instanceof int[]) rid = RelationIdentifier.get((int[]) id);
//        } catch (IllegalArgumentException e) {
//            //swallow since rid will be null and exception thrown below
//        }
//
//        TitanEdge edge = rid!=null?rid.findEdge(this):null;
//        if (null == edge)
//            throw Graph.Exceptions.elementNotFound(Edge.class, id);
//        else
//            return edge;
//    }

    @Override
    public GraphComputer compute(final Class... graphComputerClass) {
        throw new UnsupportedOperationException("Graph Computer not supported on an individual transaction. Call on graph instead.");
    }

    @Override
    public String toString() {
        return StringFactory.graphString(this, null);
    }

    @Override
    public Transaction tx() {
        return new Transaction() {
            @Override
            public void open() {
                if (isClosed()) throw new IllegalStateException("Cannot re-open a closed transaction.");
            }

            @Override
            public void commit() {
                TitanBlueprintsTransaction.this.commit();
            }

            @Override
            public void rollback() {
                TitanBlueprintsTransaction.this.rollback();
            }

            @Override
            public <R> Workload<R> submit(Function<Graph, R> graphRFunction) {
                throw new UnsupportedOperationException("Titan does not support nested transactions. " +
                        "Call submit on a TitanGraph not an individual transaction.");
            }

            @Override
            public <G extends Graph> G create() {
                throw new UnsupportedOperationException("Titan does not support nested transactions.");
            }

            @Override
            public boolean isOpen() {
                return TitanBlueprintsTransaction.this.isOpen();
            }

            @Override
            public void readWrite() {
                //Does not apply to thread-independent transactions
            }

            @Override
            public void close() {
                getGraph().tinkerpopTxContainer.close(this);
            }

            @Override
            public Transaction onReadWrite(Consumer<Transaction> transactionConsumer) {
                throw new UnsupportedOperationException("Transaction consumer can only be configured at the graph and not the transaction level.");
            }

            @Override
            public Transaction onClose(Consumer<Transaction> transactionConsumer) {
                throw new UnsupportedOperationException("Transaction consumer can only be configured at the graph and not the transaction level.");
            }
        };
    }

    @Override
    public void close() {
        tx().close();
    }


}
