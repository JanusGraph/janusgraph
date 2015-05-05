package com.thinkaurelius.titan.graphdb.vertices;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.graphdb.internal.AbstractElement;
import com.thinkaurelius.titan.graphdb.internal.ElementLifeCycle;
import com.thinkaurelius.titan.graphdb.internal.InternalVertex;
import com.thinkaurelius.titan.graphdb.query.vertex.VertexCentricQueryBuilder;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.thinkaurelius.titan.graphdb.types.VertexLabelVertex;
import com.thinkaurelius.titan.graphdb.types.system.BaseLabel;
import com.thinkaurelius.titan.graphdb.types.system.BaseVertexLabel;
import com.thinkaurelius.titan.graphdb.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.Iterator;

public abstract class AbstractVertex extends AbstractElement implements InternalVertex, Vertex {

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
    public <O> O valueOrNull(PropertyKey key) {
        return (O)property(key.name()).orElse(null);
    }

	/* ---------------------------------------------------------------
	 * Convenience Methods for TitanElement Creation
	 * ---------------------------------------------------------------
	 */

    public<V> TitanVertexProperty<V> property(final String key, final V value, final Object... keyValues) {
        TitanVertexProperty<V> p = tx().addProperty(it(), tx().getOrCreatePropertyKey(key), value);
        ElementHelper.attachProperties(p,keyValues);
        return p;
    }

    @Override
    public <V> TitanVertexProperty<V> property(final VertexProperty.Cardinality cardinality, final String key, final V value, final Object... keyValues) {
        TitanVertexProperty<V> p = tx().addProperty(cardinality, it(), tx().getOrCreatePropertyKey(key), value);
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

    public Iterator<Edge> edges(Direction direction, String... labels) {
        return (Iterator)query().direction(direction).labels(labels).edges().iterator();
    }

    public <V> Iterator<VertexProperty<V>> properties(String... keys) {
        return (Iterator)query().direction(Direction.OUT).keys(keys).properties().iterator();
    }

    public Iterator<Vertex> vertices(final Direction direction, final String... edgeLabels) {
        return (Iterator)query().direction(direction).labels(edgeLabels).vertices().iterator();

    }




}
