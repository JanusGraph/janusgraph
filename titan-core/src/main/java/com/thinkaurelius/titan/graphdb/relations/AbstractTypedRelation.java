package com.thinkaurelius.titan.graphdb.relations;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.graphdb.adjacencylist.RelationComparator;
import com.thinkaurelius.titan.graphdb.internal.*;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.tinkerpop.blueprints.Direction;

import javax.annotation.Nullable;
import java.util.Set;

public abstract class AbstractTypedRelation extends AbstractElement implements InternalRelation {

    protected final InternalType type;

    public AbstractTypedRelation(final long id, final TitanType type) {
        super(id);
        Preconditions.checkArgument(type!=null && type instanceof InternalType);
        this.type = (InternalType) type;
    }

    @Override
    public final InternalRelation it() {
        if (!getVertex(0).tx().isClosed()) return this;
        else return (InternalRelation)getId().findRelation(tx());
    }

    @Override
    public final StandardTitanTx tx() {
        StandardTitanTx tx = getVertex(0).tx();
        if (!tx.isClosed()) return tx;
        else return tx.getNextTx();
    }

	/* ---------------------------------------------------------------
	 * Immutable Aspects of Relation
	 * ---------------------------------------------------------------
	 */

    @Override
    public Direction getDirection(TitanVertex vertex) {
        for (int i=0;i<getArity();i++) {
            if (getVertex(i).equals(vertex)) return EdgeDirection.fromPosition(i);
        }
        throw new IllegalArgumentException("Relation is not incident on vertex");
    }

    @Override
    public boolean isIncidentOn(TitanVertex vertex) {
        for (int i=0;i<getArity();i++) {
            if (getVertex(i).equals(vertex)) return true;
        }
        return false;
    }

    @Override
    public boolean isHidden() {
        return type.isHidden();
    }

    @Override
    public boolean isModifiable() {
        return type.isModifiable();
    }

    @Override
    public boolean isLoop() {
        return getArity()==2 && getVertex(0).equals(getVertex(1));
    }

    @Override
    public TitanType getType() {
        return type;
    }

    @Override
    public RelationIdentifier getId() {
        return RelationIdentifier.get(this);
    }

    protected void verifyRemoval() {
        if (!isModifiable())
            throw new UnsupportedOperationException("This relation is not modifiable and hence cannot be removed");
    }

    /* ---------------------------------------------------------------
	 * Mutable Aspects of Relation
	 * ---------------------------------------------------------------
	 */

    @Override
    public Object removeProperty(String key) {
        if (!tx().containsType(key)) return null;
        else return removeProperty(tx().getType(key));
    }

    @Override
    public Object removeProperty(TitanType type) {
        return it().removePropertyDirect(type);
    }

    @Override
    public void setProperty(TitanLabel label, TitanVertex vertex) {
        it().setPropertyDirect(label,vertex);
    }

    @Override
    public void setProperty(String key, Object value) {
        it().setPropertyDirect(tx().getType(key),value);
    }

    @Override
    public void setProperty(TitanKey key, Object value) {
        it().setPropertyDirect(key,value);
    }

    @Override
    public Object getProperty(TitanKey key) {
        return it().getPropertyDirect(key);
    }

    @Override
    public Object getProperty(String key) {
        if (!tx().containsType(key)) return null;
        else return it().getPropertyDirect(tx().getType(key));
    }

    @Override
    public TitanVertex getProperty(TitanLabel label) {
        return (TitanVertex)it().getPropertyDirect(label);
    }

    @Override
    public <O> O getProperty(TitanKey key, Class<O> clazz) {
        return clazz.cast(getProperty(key));
    }

    @Override
    public <O> O getProperty(String key, Class<O> clazz) {
        return clazz.cast(getProperty(key));
    }


    @Override
    public Set<String> getPropertyKeys() {
        Set<String> result = Sets.newHashSet();
        for (TitanType type : it().getPropertyKeysDirect()) result.add(type.getName());
        return result;
    }




}
