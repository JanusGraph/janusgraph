package com.thinkaurelius.titan.graphdb.vertices;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.graphdb.query.QueryUtil;
import com.thinkaurelius.titan.graphdb.tinkerpop.optimize.TitanElementTraversal;
import com.thinkaurelius.titan.graphdb.internal.AbstractElement;
import com.thinkaurelius.titan.graphdb.internal.ElementLifeCycle;
import com.thinkaurelius.titan.graphdb.internal.InternalRelationType;
import com.thinkaurelius.titan.graphdb.internal.InternalVertex;
import com.thinkaurelius.titan.graphdb.query.vertex.VertexCentricQueryBuilder;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.thinkaurelius.titan.graphdb.types.VertexLabelVertex;
import com.thinkaurelius.titan.graphdb.types.system.BaseKey;
import com.thinkaurelius.titan.graphdb.types.system.BaseLabel;
import com.thinkaurelius.titan.graphdb.types.system.BaseVertexLabel;
import com.thinkaurelius.titan.graphdb.util.ElementHelper;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.structure.*;
import com.tinkerpop.gremlin.structure.util.StringFactory;

import javax.annotation.Nullable;
import java.util.*;
import java.util.stream.Stream;

public abstract class AbstractVertex extends AbstractElement implements InternalVertex, Vertex.Iterators {

    private final StandardTitanTx tx;


    protected AbstractVertex(StandardTitanTx tx, long id) {
        super(id);
        assert tx != null;
        this.tx = tx;
    }

    @Override
    public final InternalVertex it() {
        if (tx.isOpen())
            return this;

        InternalVertex next = (InternalVertex) tx.getNextTx().getVertex(longId());
        if (next == null) throw InvalidElementException.removedException(this);
        else return next;
    }

    @Override
    public final StandardTitanTx tx() {
        return tx.isOpen() ? tx : tx.getNextTx();
    }

    @Override
    public long getCompareId() {
        if (tx.isPartitionedVertex(this)) return tx.getIdInspector().getCanonicalVertexId(longId());
        else return longId();
    }

    @Override
    public String toString() {
        return StringFactory.vertexString(this);
    }

    @Override
    public Object id() {
        return longId();
    }

    @Override
    public boolean isModified() {
        return ElementLifeCycle.isModified(it().getLifeCycle());
    }

    protected final void verifyAccess() {
        if (isRemoved()) {
            throw InvalidElementException.removedException(this);
        }
    }

	/* ---------------------------------------------------------------
     * Changing Edges
	 * ---------------------------------------------------------------
	 */

    @Override
    public synchronized void remove() {
        verifyAccess();
//        if (isRemoved()) return; //Remove() is idempotent
        Iterator<TitanRelation> iter = it().query().noPartitionRestriction().relations().iterator();
        while (iter.hasNext()) {
            iter.next();
            iter.remove();
        }
        //Remove all system types on the vertex
        for (TitanRelation r : it().query().noPartitionRestriction().system().relations()) {
            RelationType t = r.getType();
            assert t==BaseLabel.VertexLabelEdge || t==BaseKey.VertexExists;
            r.remove();
        }
    }

	/* ---------------------------------------------------------------
	 * TitanRelation Iteration/Access
	 * ---------------------------------------------------------------
	 */

    @Override
    public String label() {
        return vertexLabel().name();
    }

    protected Vertex getVertexLabelInternal() {
        return Iterables.getOnlyElement(tx().query(this).noPartitionRestriction().type(BaseLabel.VertexLabelEdge).direction(Direction.OUT).vertices(),null);
    }

    @Override
    public VertexLabel vertexLabel() {
        Vertex label = getVertexLabelInternal();
        if (label==null) return BaseVertexLabel.DEFAULT_VERTEXLABEL;
        else return (VertexLabelVertex)label;
    }

    @Override
    public VertexCentricQueryBuilder query() {
        verifyAccess();
        return tx().query(this);
    }

    @Override
    public <O> O valueOrNull(RelationType key) {
        return (O)property(key.name()).orElse(null);
    }

    @Override
    public GraphTraversal<Vertex, Vertex> start() {
        return new TitanElementTraversal<>(it(), tx());
    }

	/* ---------------------------------------------------------------
	 * Convenience Methods for TitanElement Creation
	 * ---------------------------------------------------------------
	 */

    @Override
    public<V> TitanVertexProperty<V> property(String key, V value) {
        return tx().addProperty(it(), tx().getOrCreatePropertyKey(key), value);
    }

    @Override
    public <V> TitanVertexProperty<V> singleProperty(String key, V value, Object... keyValues) {
        TitanVertexProperty<V> p = tx().setProperty(it(),tx().getOrCreatePropertyKey(key),value);
        ElementHelper.attachProperties(p,keyValues);
        return p;
    }

    @Override
    public TitanEdge addEdge(String label, Vertex vertex, Object... keyValues) {
        Preconditions.checkArgument(vertex instanceof TitanVertex,"Invalid vertex provided: %s",vertex);
        TitanEdge edge = tx().addEdge(it(), (TitanVertex) vertex, tx().getOrCreateEdgeLabel(label));
        ElementHelper.attachProperties(edge,keyValues);
        return edge;
    }

	/* ---------------------------------------------------------------
	 * TinkerPop Iterators Method
	 * ---------------------------------------------------------------
	 */

    @Override
    public Vertex.Iterators iterators() {
        return this;
    }

    @Override
    public Iterator<Edge> edgeIterator(Direction direction, String... strings) {
        return (Iterator)query().direction(direction).labels(strings).edges().iterator();
    }

    @Override
    public Iterator<Vertex> vertexIterator(Direction direction, String... strings) {
        return (Iterator)query().direction(direction).labels(strings).vertices().iterator();
    }

    @Override
    public <V> Iterator<VertexProperty<V>> propertyIterator(String... keys) {
        return (Iterator)query().direction(Direction.OUT).keys(keys).properties().iterator();
    }

}
