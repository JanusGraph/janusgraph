package com.thinkaurelius.titan.graphdb.vertices;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.core.Cardinality;
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
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.StringFactory;

import java.util.*;

public abstract class AbstractVertex extends AbstractElement implements InternalVertex {

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

        InternalVertex next = (InternalVertex) tx.getNextTx().getVertex(getLongId());
        if (next == null) throw InvalidElementException.removedException(this);
        else return next;
    }

    @Override
    public final StandardTitanTx tx() {
        return tx.isOpen() ? tx : tx.getNextTx();
    }

    @Override
    public long getCompareId() {
        if (tx.isPartitionedVertex(this)) return tx.getIdInspector().getCanonicalVertexId(getLongId());
        else return getLongId();
    }

    @Override
    public String toString() {
        return StringFactory.vertexString(this);
    }

    @Override
    public Object getId() {
        return getLongId();
    }

    @Override
    public boolean isModified() {
        return ElementLifeCycle.isModified(it().getLifeCycle());
    }




	/* ---------------------------------------------------------------
     * Changing Edges
	 * ---------------------------------------------------------------
	 */

    @Override
    public synchronized void remove() {
        if (isRemoved()) throw InvalidElementException.removedException(this);
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
    public String getLabel() {
        return getVertexLabel().getName();
    }

    protected Vertex getVertexLabelInternal() {
        return Iterables.getOnlyElement(tx().query(this).noPartitionRestriction().type(BaseLabel.VertexLabelEdge).direction(Direction.OUT).vertices(),null);
    }

    @Override
    public VertexLabel getVertexLabel() {
        Vertex label = getVertexLabelInternal();
        if (label==null) return BaseVertexLabel.DEFAULT_VERTEXLABEL;
        else return (VertexLabelVertex)label;
    }

    @Override
    public VertexCentricQueryBuilder query() {
        Preconditions.checkArgument(!isRemoved(), "Cannot access a removed vertex: %s", this);
        return tx().query(this);
    }

    @Override
    public Set<String> getPropertyKeys() {
        Set<String> result = new HashSet<String>();
        for (TitanProperty p : getProperties()) {
            result.add(p.getPropertyKey().getName());
        }
        return result;
    }

    @Override
    public <O> O getProperty(PropertyKey key) {
        if (!((InternalRelationType)key).isHiddenType() && tx().getConfiguration().hasPropertyPrefetching()) {
            getProperties().iterator().hasNext();
        }
        Iterator<TitanProperty> iter = query().type(key).properties().iterator();
        if (key.getCardinality()== Cardinality.SINGLE) {
            if (iter.hasNext()) return (O)iter.next().getValue();
            else return null;
        } else {
            List<Object> result = new ArrayList<Object>();
            while (iter.hasNext()) {
                result.add(iter.next().getValue());
            }
            return (O)result;
        }
    }

    @Override
    public <O> O getProperty(String key) {
        if (!tx().containsRelationType(key)) return null;
        else return getProperty(tx().getPropertyKey(key));
    }

    @Override
    public Iterable<TitanProperty> getProperties() {
        return query().properties();
    }

    @Override
    public Iterable<TitanProperty> getProperties(PropertyKey key) {
        return query().type(key).properties();
    }

    @Override
    public Iterable<TitanProperty> getProperties(String key) {
        return query().keys(key).properties();
    }


    @Override
    public Iterable<TitanEdge> getEdges() {
        return query().titanEdges();
    }


    @Override
    public Iterable<TitanEdge> getTitanEdges(Direction dir, EdgeLabel... labels) {
        return query().direction(dir).types(labels).titanEdges();
    }

    @Override
    public Iterable<Edge> getEdges(Direction dir, String... labels) {
        return query().direction(dir).labels(labels).edges();
    }

    @Override
    public Iterable<TitanRelation> getRelations() {
        return query().relations();
    }

    @Override
    public Iterable<Vertex> getVertices(Direction direction, String... labels) {
        return query().direction(direction).labels(labels).vertices();
    }

	
	/* ---------------------------------------------------------------
	 * TitanRelation Counts
	 * ---------------------------------------------------------------
	 */


    @Override
    public long getPropertyCount() {
        return query().propertyCount();
    }


    @Override
    public long getEdgeCount() {
        return query().count();
    }

    @Override
    public boolean isConnected() {
        return !Iterables.isEmpty(getEdges());
    }

	
	/* ---------------------------------------------------------------
	 * Convenience Methods for TitanElement Creation
	 * ---------------------------------------------------------------
	 */

    @Override
    public TitanProperty addProperty(PropertyKey key, Object attribute) {
        return tx().addProperty(it(), key, attribute);
    }


    @Override
    public TitanProperty addProperty(String key, Object attribute) {
        return tx().addProperty(it(), key, attribute);
    }

    @Override
    public void setProperty(String key, Object value) {
        setProperty(tx().getOrCreatePropertyKey(key),value);
    }

    @Override
    public void setProperty(final PropertyKey key, Object value) {
        tx().setProperty(it(),key,value);
    }


    @Override
    public TitanEdge addEdge(EdgeLabel label, TitanVertex vertex) {
        return tx().addEdge(it(), vertex, label);
    }

    @Override
    public TitanEdge addEdge(String label, TitanVertex vertex) {
        return tx().addEdge(it(), vertex, label);
    }

    @Override
    public Edge addEdge(String label, Vertex vertex) {
        return addEdge(label,(TitanVertex)vertex);
    }

    @Override
    public <O> O removeProperty(RelationType key) {
        assert key.isPropertyKey();

        Object result = null;
        for (TitanProperty p : query().type(key).properties()) {
            result = p.getValue();
            p.remove();
        }
        return (O) result;
    }

    @Override
    public <O> O removeProperty(String key) {
        if (!tx().containsRelationType(key)) return null;
        else return removeProperty(tx().getPropertyKey(key));
    }
}
