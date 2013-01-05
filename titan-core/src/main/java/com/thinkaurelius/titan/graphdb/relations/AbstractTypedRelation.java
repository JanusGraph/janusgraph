package com.thinkaurelius.titan.graphdb.relations;

import com.thinkaurelius.titan.core.TitanLabel;
import com.thinkaurelius.titan.core.TitanProperty;
import com.thinkaurelius.titan.core.TitanType;
import com.thinkaurelius.titan.graphdb.adjacencylist.RelationComparator;
import com.thinkaurelius.titan.graphdb.types.InternalTitanType;
import com.thinkaurelius.titan.graphdb.vertices.NewEmptyTitanVertex;

public abstract class AbstractTypedRelation extends NewEmptyTitanVertex implements InternalRelation {

    protected final InternalTitanType type;

    public AbstractTypedRelation(TitanType type) {
        assert type != null;
        assert type instanceof InternalTitanType;
        this.type = (InternalTitanType) type;
    }

	
	/* ---------------------------------------------------------------
	 * In memory handling
	 * ---------------------------------------------------------------
	 */

    @Override
    public int hashCode() {
        throw new UnsupportedOperationException("Needs to be overwritten");
    }

    protected final int objectHashCode() {
        return super.hashCode();
    }

    @Override
    public boolean equals(Object oth) {
        if (oth == this) return true;
        else if (!(oth instanceof InternalRelation)) return false;
        InternalRelation other = (InternalRelation) oth;
        if (hasID() && other.hasID()) return getID() == other.getID();
        else if (hasID() || other.hasID()) return false;
        if (!type.equals(other.getType())) return false;
        if (!getVertex(0).equals(other.getVertex(0))) return false;
        for (String key : type.getDefinition().getKeySignature()) {
            int keycompare = RelationComparator.compareOnKey(this, other, key);
            if (keycompare != 0) return false;
        }
        if (type.isFunctional()) return true;
        if (type.isPropertyKey()) {
            return ((TitanProperty) this).getAttribute().equals(((TitanProperty) other).getAttribute());
        } else {
            return getVertex(1).equals(other.getVertex(1));
        }
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        throw new CloneNotSupportedException("To be implemented");
    }

    @Override
    public void remove() {
        if (!isModifiable())
            throw new UnsupportedOperationException("This edge is unmodifiable and hence cannot be deleted");
        forceDelete();
    }

    @Override
    public synchronized void forceDelete() {
        getTransaction().deletedRelation(this);
    }

    @Override
    public boolean isInline() {
        return false;
    }
	
	/* ---------------------------------------------------------------
	 * TitanType methods
	 * ---------------------------------------------------------------
	 */

    @Override
    public TitanType getType() {
        return type;
    }

    @Override
    public boolean isUndirected() {
        return !type.isPropertyKey() && ((TitanLabel) type).isUndirected();
    }

    @Override
    public boolean isDirected() {
        return type.isPropertyKey() || ((TitanLabel) type).isDirected();
    }

    @Override
    public boolean isUnidirected() {
        return !type.isPropertyKey() && ((TitanLabel) type).isUnidirected();
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
    public boolean isSimple() {
        return type.isSimple();
    }


}
