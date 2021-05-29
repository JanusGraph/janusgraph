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

package org.janusgraph.graphdb.tinkerpop;

import com.google.common.base.Preconditions;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.Io;
import org.apache.tinkerpop.gremlin.structure.util.AbstractThreadedTransaction;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.core.VertexLabel;
import org.janusgraph.diskstorage.util.Hex;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.olap.computer.FulgoraGraphComputer;
import org.janusgraph.graphdb.relations.RelationIdentifier;
import org.janusgraph.graphdb.types.system.BaseVertexLabel;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Optional;

/**
 * Blueprints specific implementation of {@link JanusGraphTransaction}.
 * Provides utility methods that wrap JanusGraph calls with Blueprints terminology.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public abstract class JanusGraphBlueprintsTransaction implements JanusGraphTransaction {

    /**
     * Returns the graph that this transaction is based on
     * @return
     */
    protected abstract JanusGraphBlueprintsGraph getGraph();

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
        JanusGraphBlueprintsGraph graph = getGraph();
        if (isOpen()) commit();
        return graph.compute(graphComputerClass);
    }

    @Override
    public FulgoraGraphComputer compute() throws IllegalArgumentException {
        JanusGraphBlueprintsGraph graph = getGraph();
        if (isOpen()) commit();
        return graph.compute();
    }

    /**
     * Creates a new vertex in the graph with the given vertex id.
     * Note, that an exception is thrown if the vertex id is not a valid JanusGraph vertex id or if a vertex with the given
     * id already exists. Only accepts long ids - all others are ignored.
     * <p>
     * Custom id setting must be enabled via the configuration option {@link org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration#ALLOW_SETTING_VERTEX_ID}
     * and valid JanusGraph vertex ids must be provided. Use {@link org.janusgraph.graphdb.idmanagement.IDManager#toVertexId(long)}
     * to construct a valid JanusGraph vertex id from a user id, where <code>idManager</code> can be obtained through
     * {@link org.janusgraph.graphdb.database.StandardJanusGraph#getIDManager()}.
     * <pre>
     * <code>long vertexId = ((StandardJanusGraph) graph).getIDManager().toVertexId(userVertexId);</code>
     * </pre>
     *
     * @param keyValues key-value pairs of properties to characterize or attach to the vertex
     * @return New vertex
     */
    @Override
    public JanusGraphVertex addVertex(Object... keyValues) {
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        final Optional<Object> idValue = ElementHelper.getIdValue(keyValues);
        if (idValue.isPresent() && !((StandardJanusGraph) getGraph()).getConfiguration().allowVertexIdSetting()) throw Vertex.Exceptions.userSuppliedIdsNotSupported();
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

        final Long id = idValue.map(Number.class::cast).map(Number::longValue).orElse(null);
        final JanusGraphVertex vertex = addVertex(id, label);
        org.janusgraph.graphdb.util.ElementHelper.attachProperties(vertex, keyValues);
        return vertex;
    }

    @Override
    public Iterator<Vertex> vertices(Object... vertexIds) {
        if (vertexIds==null || vertexIds.length==0) return (Iterator)getVertices().iterator();
        ElementUtils.verifyArgsMustBeEitherIdOrElement(vertexIds);
        long[] ids = new long[vertexIds.length];
        int pos = 0;
        for (Object vertexId : vertexIds) {
            long id = ElementUtils.getVertexId(vertexId);
            if (id > 0) ids[pos++] = id;
        }
        if (pos==0) return Collections.emptyIterator();
        if (pos<ids.length) ids = Arrays.copyOf(ids,pos);
        return (Iterator)getVertices(ids).iterator();
    }

    @Override
    public Iterator<Edge> edges(Object... edgeIds) {
        if (edgeIds==null || edgeIds.length==0) return (Iterator)getEdges().iterator();
        ElementUtils.verifyArgsMustBeEitherIdOrElement(edgeIds);
        RelationIdentifier[] ids = new RelationIdentifier[edgeIds.length];
        int pos = 0;
        for (Object edgeId : edgeIds) {
            RelationIdentifier id = ElementUtils.getEdgeId(edgeId);
            if (id != null) ids[pos++] = id;
        }
        if (pos==0) return Collections.emptyIterator();
        if (pos<ids.length) ids = Arrays.copyOf(ids,pos);
        return (Iterator)getEdges(ids).iterator();
    }

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
        return new AbstractThreadedTransaction(getGraph()) {
            @Override
            public void doOpen() {
                if (isClosed()) throw new IllegalStateException("Cannot re-open a closed transaction.");
            }

            @Override
            public void doCommit() {
                JanusGraphBlueprintsTransaction.this.commit();
            }

            @Override
            public void doRollback() {
                JanusGraphBlueprintsTransaction.this.rollback();
            }

            @Override
            public <G extends Graph> G createThreadedTx() {
                throw new UnsupportedOperationException("JanusGraph does not support nested transactions.");
            }

            @Override
            public boolean isOpen() {
                return JanusGraphBlueprintsTransaction.this.isOpen();
            }

            @Override
            protected void doClose() {
                if (isOpen()) {
                    throw Exceptions.openTransactionsOnClose();
                }
                super.doClose();
            }
        };
    }

    @Override
    public void close() {
        tx().close();
    }


}
