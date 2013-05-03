package com.thinkaurelius.titan.graphdb.blueprints;

import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import com.thinkaurelius.titan.graphdb.util.ExceptionFactory;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Parameter;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.StringFactory;

import java.util.Set;
import java.util.WeakHashMap;

/**
 * Blueprints specific implementation for {@link TitanGraph}.
 * Handles thread-bound transactions.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public abstract class TitanBlueprintsGraph implements TitanGraph {

    // ########## TRANSACTION HANDLING ###########################

    private ThreadLocal<TitanTransaction> txs = new ThreadLocal<TitanTransaction>() {

        protected TitanTransaction initialValue() {
            return null;
        }

    };

    private final WeakHashMap<TitanTransaction, Boolean> openTx = new WeakHashMap<TitanTransaction, Boolean>(4);

    @Override
    public void commit() {
        TitanTransaction tx = txs.get();
        if (tx != null && tx.isOpen()) {
            try {
                tx.commit();
            } finally {
                txs.remove();
                openTx.remove(tx);
            }
        }
    }

    @Override
    public void rollback() {
        TitanTransaction tx = txs.get();
        if (tx != null && tx.isOpen()) {
            try {
                tx.rollback();
            } finally {
                txs.remove();
                openTx.remove(tx);
            }
        }
    }

    @Override
    public void stopTransaction(Conclusion conclusion) {
        switch (conclusion) {
            case SUCCESS:
                commit();
                break;
            case FAILURE:
                rollback();
                break;
            default:
                throw new IllegalArgumentException("Unrecognized conclusion: " + conclusion);
        }
    }

    public abstract TitanTransaction newThreadBoundTransaction();

    private TitanTransaction getAutoStartTx() {
        if (txs==null)  ExceptionFactory.graphShutdown();
        TitanTransaction tx = txs.get();
        if (tx == null) {
            tx = newThreadBoundTransaction();
            txs.set(tx);
            openTx.put(tx, Boolean.TRUE);
        }
        return tx;
    }

    public TitanTransaction getCurrentThreadTx() {
        return getAutoStartTx();
    }


    @Override
    public synchronized void shutdown() {
        for (TitanTransaction tx : openTx.keySet()) {
            tx.commit();
        }
        openTx.clear();
        txs = null;
    }

    @Override
    public String toString() {
        GraphDatabaseConfiguration config = ((StandardTitanGraph) this).getConfiguration();
        return "titangraph" + StringFactory.L_BRACKET +
                config.getBackendDescription() + StringFactory.R_BRACKET;
//        return StringFactory.graphString(this,config.getBackendDescription());
    }


    // ########## INDEX HANDLING ###########################

    @Override
    public <T extends Element> void dropKeyIndex(String key, Class<T> elementClass) {
        throw new UnsupportedOperationException("Key indexes cannot be dropped");
    }

    @Override
    public <T extends Element> void createKeyIndex(String key, Class<T> elementClass, final Parameter... indexParameters) {
        getAutoStartTx().createKeyIndex(key, elementClass);
    }

    @Override
    public <T extends Element> Set<String> getIndexedKeys(Class<T> elementClass) {
        return getAutoStartTx().getIndexedKeys(elementClass);
    }

    // ########## TRANSACTIONAL FORWARDING ###########################

    @Override
    public Vertex addVertex(Object id) {
        return getAutoStartTx().addVertex(id);
    }

    @Override
    public Vertex getVertex(Object id) {
        return getAutoStartTx().getVertex(id);
    }

    @Override
    public void removeVertex(Vertex vertex) {
        getAutoStartTx().removeVertex(vertex);
    }

    @Override
    public Iterable<Vertex> getVertices() {
        return getAutoStartTx().getVertices();
    }

    @Override
    public TypeMaker makeType() {
        return getAutoStartTx().makeType();
    }

    @Override
    public TitanGraphQuery query() {
        return getAutoStartTx().query();
    }

    @Override
    public TitanType getType(String name) {
        return getAutoStartTx().getType(name);
    }

    @Override
    public Iterable<Vertex> getVertices(String key, Object value) {
        return getAutoStartTx().getVertices(key, value);
    }

    @Override
    public Edge addEdge(Object id, Vertex outVertex, Vertex inVertex, String label) {
        return getAutoStartTx().addEdge(id, outVertex, inVertex, label);
    }

    @Override
    public Edge getEdge(Object id) {
        return getAutoStartTx().getEdge(id);
    }

    @Override
    public void removeEdge(Edge edge) {
        getAutoStartTx().removeEdge(edge);
    }

    @Override
    public Iterable<Edge> getEdges() {
        return getAutoStartTx().getEdges();
    }

    @Override
    public Iterable<Edge> getEdges(String key, Object value) {
        return getAutoStartTx().getEdges(key, value);
    }

}
