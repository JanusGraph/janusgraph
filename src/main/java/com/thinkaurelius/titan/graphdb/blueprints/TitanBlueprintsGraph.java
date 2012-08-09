package com.thinkaurelius.titan.graphdb.blueprints;

import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.database.InternalTitanGraph;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import com.tinkerpop.blueprints.*;
import com.tinkerpop.blueprints.util.StringFactory;

import java.util.Set;
import java.util.WeakHashMap;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public abstract class TitanBlueprintsGraph implements InternalTitanGraph {

    // ########## TRANSACTION HANDLING ###########################
    
    private ThreadLocal<TitanTransaction> txs =  new ThreadLocal<TitanTransaction>() {

        protected TitanTransaction initialValue() {
            return null;
        }

    };

    private final WeakHashMap<TitanTransaction,Boolean> openTx = new WeakHashMap<TitanTransaction, Boolean>(4);

    @Override
    public void stopTransaction(final Conclusion conclusion) {
        TitanTransaction tx = txs.get();
        if (tx!=null) {
            assert tx.isOpen();
            try {
                tx.stopTransaction(conclusion);
            } finally {
                txs.remove();
                openTx.remove(tx);
            }
        }
    }

    private TitanTransaction internalStartTransaction() {
        TitanTransaction tx = (TitanTransaction) startTransaction();
        txs.set(tx);
        openTx.put(tx,Boolean.TRUE);
        return tx;
    }

    private TitanTransaction getAutoStartTx() {
        TitanTransaction tx = txs.get();
        if (tx==null) {
            tx=internalStartTransaction();
        }
        return tx;
    }


    @Override
    public synchronized void shutdown() {
        for (TitanTransaction tx : openTx.keySet()) {
            tx.commit();
        }
        openTx.clear();
        txs=null;
    }
    
    @Override
    public String toString() {
        GraphDatabaseConfiguration config = ((StandardTitanGraph)this).getConfiguration();
        return "titangraph" + StringFactory.L_BRACKET +
                config.getStorageManagerDescription() + StringFactory.R_BRACKET;
//        return StringFactory.graphString(this,config.getStorageManagerDescription());
    }


    // ########## INDEX HANDLING ###########################

    @Override
    public <T extends Element> void dropKeyIndex(String key, Class<T> elementClass) {
        throw new UnsupportedOperationException("Key indexes cannot be dropped");
    }

    @Override
    public <T extends Element> void createKeyIndex(String key, Class<T> elementClass) {
        getAutoStartTx().createKeyIndex(key,elementClass);
    }

    @Override
    public <T extends Element> Set<String> getIndexedKeys(Class<T> elementClass) {
        return getAutoStartTx().getIndexedKeys(elementClass);
    }

    // ########## FEATURES ###########################
    
    @Override
    public Features getFeatures() {
        Features features = TitanFeatures.getBaselineTitanFeatures();
        GraphDatabaseConfiguration config = ((StandardTitanGraph)this).getConfiguration();
        features.supportsSerializableObjectProperty = config.hasSerializeAll();
        return features;
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
    public TitanType getType(String name) {
        return getAutoStartTx().getType(name);
    }

    @Override
    public Iterable<Vertex> getVertices(String key, Object value) {
        return getAutoStartTx().getVertices(key,value);
    }

    @Override
    public Edge addEdge(Object id, Vertex outVertex, Vertex inVertex, String label) {
        return getAutoStartTx().addEdge(id,outVertex,inVertex,label);
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
        return getAutoStartTx().getEdges(key,value);
    }

}
