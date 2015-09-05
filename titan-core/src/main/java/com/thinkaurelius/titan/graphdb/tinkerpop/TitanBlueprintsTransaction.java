package com.thinkaurelius.titan.graphdb.tinkerpop;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.TitanTransaction;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.core.VertexLabel;
import com.thinkaurelius.titan.diskstorage.util.Hex;
import com.thinkaurelius.titan.graphdb.olap.computer.FulgoraGraphComputer;
import com.thinkaurelius.titan.graphdb.relations.RelationIdentifier;
import com.thinkaurelius.titan.graphdb.types.system.BaseVertexLabel;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.io.Io;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.structure.T;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
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

    @Override
    public <I extends Io> I io(final Io.Builder<I> builder) {
        return getGraph().io(builder);
    }

    @Override
    public <C extends GraphComputer> C compute(Class<C> graphComputerClass) throws IllegalArgumentException {
        return getGraph().compute(graphComputerClass);
    }

    @Override
    public FulgoraGraphComputer compute() throws IllegalArgumentException {
        return getGraph().compute();
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
//        for (int i = 0; i < keyValues.length; i = i + 2) {
//            if (!keyValues[i].equals(T.id) && !keyValues[i].equals(T.label))
//                ((StandardTitanTx)this).addPropertyInternal(vertex,getOrCreatePropertyKey((String) keyValues[i]),keyValues[i+1]);
//        }
        com.thinkaurelius.titan.graphdb.util.ElementHelper.attachProperties(vertex, keyValues);
        return vertex;
    }

    @Override
    public Iterator<Vertex> vertices(Object... vids) {
        if (vids==null || vids.length==0) return (Iterator)getVertices().iterator();
        ElementUtils.verifyArgsMustBeEitherIdorElement(vids);
        long[] ids = new long[vids.length];
        int pos = 0;
        for (int i = 0; i < vids.length; i++) {
            long id = ElementUtils.getVertexId(vids[i]);
            if (id>0) ids[pos++]=id;
        }
        if (pos==0) return Collections.emptyIterator();
        if (pos<ids.length) ids = Arrays.copyOf(ids,pos);
        return (Iterator)getVertices(ids).iterator();
    }

    @Override
    public Iterator<Edge> edges(Object... eids) {
        if (eids==null || eids.length==0) return (Iterator)getEdges().iterator();
        ElementUtils.verifyArgsMustBeEitherIdorElement(eids);
        RelationIdentifier[] ids = new RelationIdentifier[eids.length];
        int pos = 0;
        for (int i = 0; i < eids.length; i++) {
            RelationIdentifier id = ElementUtils.getEdgeId(eids[i]);
            if (id!=null) ids[pos++]=id;
        }
        if (pos==0) return Collections.emptyIterator();
        if (pos<ids.length) ids = Arrays.copyOf(ids,pos);
        return (Iterator)getEdges(ids).iterator();
    }




//    @Override
//    public GraphComputer compute(final Class... graphComputerClass) {
//        throw new UnsupportedOperationException("Graph Computer not supported on an individual transaction. Call on graph instead.");
//    }

    @Override
    public String toString() {
        int ihc = System.identityHashCode(this);
        String ihcString = String.format("0x%s", Hex.bytesToHex(
                (byte)(ihc >>> 24 & 0x000000FF),
                (byte)(ihc >>> 16 & 0x000000FF),
                (byte)(ihc >>> 8  & 0x000000FF),
                (byte)(ihc        & 0x000000FF)));
        return StringFactory.graphString(this, ihcString);
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
            public <G extends Graph> G createThreadedTx() {
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

            @Override
            public void addTransactionListener(Consumer<Status> listener) {
                throw new UnsupportedOperationException("Transaction consumer can only be configured at the graph and not the transaction level.");
            }

            @Override
            public void removeTransactionListener(Consumer<Status> listener) {
                throw new UnsupportedOperationException("Transaction consumer can only be configured at the graph and not the transaction level.");
            }

            @Override
            public void clearTransactionListeners() {
                // Could issue a warning here
            }
        };
    }

    @Override
    public void close() {
        tx().close();
    }


}
