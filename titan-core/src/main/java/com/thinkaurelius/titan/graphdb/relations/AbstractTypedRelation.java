package com.thinkaurelius.titan.graphdb.relations;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.thinkaurelius.titan.core.TitanKey;
import com.thinkaurelius.titan.core.TitanLabel;
import com.thinkaurelius.titan.core.TitanType;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.graphdb.internal.AbstractElement;
import com.thinkaurelius.titan.graphdb.internal.InternalRelation;
import com.thinkaurelius.titan.graphdb.internal.InternalType;
import com.thinkaurelius.titan.graphdb.internal.InternalVertex;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.tinkerpop.blueprints.Direction;

import java.util.Set;

public abstract class AbstractTypedRelation extends AbstractElement implements InternalRelation {

    protected final InternalType type;

    public AbstractTypedRelation(final long id, final TitanType type) {
        super(id);
        Preconditions.checkArgument(type!=null && type instanceof InternalType);
        this.type = (InternalType) type;
    }

    @Override
    public InternalRelation it() {
        InternalVertex v = getVertex(0);
        if (v==v.it()) return this;
        else return (InternalRelation)getId().findRelation(tx());
    }

    @Override
    public final StandardTitanTx tx() {
        return getVertex(0).tx();
    }

	/* ---------------------------------------------------------------
	 * Immutable Aspects of Relation
	 * ---------------------------------------------------------------
	 */

    @Override
    public Direction getDirection(TitanVertex vertex) {
        for (int i=0;i<getArity();i++) {
            if (it().getVertex(i).equals(vertex)) return EdgeDirection.fromPosition(i);
        }
        throw new IllegalArgumentException("Relation is not incident on vertex");
    }

    @Override
    public boolean isIncidentOn(TitanVertex vertex) {
        for (int i=0;i<getArity();i++) {
            if (it().getVertex(i).equals(vertex)) return true;
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
        return getArity()==2 && it().getVertex(0).equals(it().getVertex(1));
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
        Preconditions.checkArgument(!it().isRemoved(),"Cannot modified removed relation");
        return it().removePropertyDirect(type);
    }

    @Override
    public void setProperty(TitanLabel label, TitanVertex vertex) {
        Preconditions.checkArgument(!it().isRemoved(),"Cannot modified removed relation");
        Preconditions.checkArgument(label.isUnidirected(),"Label must be unidirected");
        Preconditions.checkArgument(label.isUnique(Direction.OUT),"Label must have unique end point");
        it().setPropertyDirect(label,vertex);
    }

    @Override
    public void setProperty(TitanKey key, Object value) {
        Preconditions.checkArgument(!it().isRemoved(),"Cannot modified removed relation");
        Preconditions.checkArgument(key.isUnique(Direction.OUT),"Key must have unique assignment");
        it().setPropertyDirect(key,AttributeUtil.verifyAttribute(key,value));
    }

    @Override
    public void setProperty(String key, Object value) {
        TitanType type = tx().getType(key);
        if (type instanceof TitanKey) setProperty((TitanKey)type,value);
        else if (type instanceof TitanLabel) {
            Preconditions.checkArgument(value instanceof TitanVertex,"Value must be a vertex");
            setProperty((TitanLabel) type, (InternalVertex) value);
        }
    }

    @Override
    public Object getProperty(TitanKey key) {
        return it().getPropertyDirect(key);
    }

    @Override
    public Object getProperty(String key) {
        if (!tx().containsType(key)) return null;
        TitanType type = tx().getType(key);
        if (type==null) return null;
        else if (type.isPropertyKey()) return getProperty((TitanKey) type);
        else return getProperty((TitanLabel)type);
    }

    @Override
    public TitanVertex getProperty(TitanLabel label) {
        Object val = it().getPropertyDirect(label);
        if (val==null) return null;
        else if (val instanceof Number) return tx().getExistingVertex(((Number)val).longValue());
        else if (val instanceof TitanVertex) return (TitanVertex) val;
        else throw new IllegalStateException("Invalid object found instead of vertex: " + val);
    }

    @Override
    public Object getProperty(TitanType type) {
        if (type.isEdgeLabel()) return getProperty((TitanLabel)type);
        else return getProperty((TitanKey)type);
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
