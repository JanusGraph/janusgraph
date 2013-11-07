package com.thinkaurelius.titan.graphdb.vertices;

import com.google.common.collect.Iterables;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.graphdb.internal.AbstractElement;
import com.thinkaurelius.titan.graphdb.internal.ElementLifeCycle;
import com.thinkaurelius.titan.graphdb.internal.InternalVertex;
import com.thinkaurelius.titan.graphdb.query.QueryUtil;
import com.thinkaurelius.titan.graphdb.query.VertexCentricQueryBuilder;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.thinkaurelius.titan.graphdb.types.system.SystemKey;
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

        InternalVertex next = (InternalVertex) tx.getNextTx().getVertex(getID());
        if (next == null)
            throw new InvalidElementException("Vertex has been removed", this);

        else return next;
    }

    @Override
    public final StandardTitanTx tx() {
        return tx.isOpen() ? tx : tx.getNextTx();
    }

    @Override
    public String toString() {
        return StringFactory.vertexString(this);
    }

    @Override
    public Object getId() {
        return getID();
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
        if (it().isRemoved()) return;
        //TODO: It's Blueprints semantics to remove all edges - is this correct?
        Iterator<TitanRelation> iter = it().getRelations().iterator();
        while (iter.hasNext()) {
            iter.next();
            iter.remove();
        }
        //Finally remove internal/hidden relations
        for (TitanRelation r : QueryUtil.queryAll(it())) {
            if (r.getType().equals(SystemKey.VertexState)) r.remove();
            else throw new IllegalStateException("Cannot remove vertex since it is still connected");
        }
    }

	/* ---------------------------------------------------------------
	 * TitanRelation Iteration/Access
	 * ---------------------------------------------------------------
	 */

    @Override
    public VertexCentricQueryBuilder query() {
        return tx().query(it());
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
    public <O> O getProperty(TitanKey key) {
        Iterator<TitanProperty> iter = query().type(key).includeHidden().properties().iterator();
        if (key.isUnique(Direction.OUT)) {
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
        if (!tx().containsType(key)) return null;
        else return getProperty(tx().getPropertyKey(key));
    }

    @Override
    public Iterable<TitanProperty> getProperties() {
        return query().properties();
    }

    @Override
    public Iterable<TitanProperty> getProperties(TitanKey key) {
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
    public Iterable<TitanEdge> getTitanEdges(Direction dir, TitanLabel... labels) {
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
    public TitanProperty addProperty(TitanKey key, Object attribute) {
        return tx().addProperty(it(), key, attribute);
    }


    @Override
    public TitanProperty addProperty(String key, Object attribute) {
        return tx().addProperty(it(), key, attribute);
    }

    @Override
    public void setProperty(String key, Object value) {
        setProperty(tx().getPropertyKey(key),value);
    }

    @Override
    public void setProperty(final TitanKey key, Object value) {
        tx().setProperty(it(),key,value);
    }


    @Override
    public TitanEdge addEdge(TitanLabel label, TitanVertex vertex) {
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
    public <O> O removeProperty(TitanType key) {
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
        if (!tx().containsType(key)) return null;
        else return removeProperty(tx().getPropertyKey(key));
    }

    @Override
    public boolean equals(Object other) {
        if (other == null)
            return false;

        if (this == other)
            return true;

        try {
            return id == ((AbstractVertex) other).id;
        } catch (ClassCastException e) {
            return super.equals(other);
        }
    }
    
    /*
     * This silences a static analysis warning about overridding equals without
     * overridding hashcode.
     */
    @Override
    public int hashCode() {
        return super.hashCode();
    }
}
