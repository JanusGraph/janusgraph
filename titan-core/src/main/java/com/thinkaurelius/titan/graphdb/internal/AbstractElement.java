package com.thinkaurelius.titan.graphdb.internal;

import com.google.common.primitives.Longs;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.graphdb.idmanagement.IDManager;

/**
 * AbstractElement is the base class for all elements in Titan.
 * It is defined and uniquely identified by its id.
 * </p>
 * For the id, it holds that:
 * id<0: Temporary id, will be assigned id>0 when the transaction is committed
 * id=0: Virtual or implicit element that does not physically exist in the database
 * id>0: Physically persisted element
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public abstract class AbstractElement implements InternalElement, Comparable<TitanElement> {

    private long id;

    public AbstractElement(long id) {
        this.id = id;
    }

    public static boolean isTemporaryId(long elementId) {
        return elementId < 0;
    }

    @Override
    public int hashCode() {
        return Longs.hashCode(getCompareId());
    }

    @Override
    public boolean equals(Object other) {
        if (other == null)
            return false;

        if (this == other)
            return true;

        if (other instanceof AbstractElement) {
            if (getCompareId()!=((AbstractElement)other).getCompareId()) return false;
        } else if (other instanceof TitanElement) {
            if (getCompareId()!=((TitanElement)other).getLongId()) return false;
        } else return false;

        if (this instanceof TitanVertex && other instanceof TitanVertex)
            return true;

        if (this instanceof TitanRelation && other instanceof TitanRelation)
            return true;

        return false;
    }


    @Override
    public int compareTo(TitanElement other) {
        return compare(this,other);
    }

    public static int compare(TitanElement e1, TitanElement e2) {
        long e1id = (e1 instanceof AbstractElement)?((AbstractElement)e1).getCompareId():e1.getLongId();
        long e2id = (e2 instanceof AbstractElement)?((AbstractElement)e2).getCompareId():e2.getLongId();
        return Longs.compare(e1id,e2id);
    }

    @Override
    public InternalVertex clone() throws CloneNotSupportedException {
        throw new CloneNotSupportedException();
    }

    /* ---------------------------------------------------------------
	 * ID and LifeCycle methods
	 * ---------------------------------------------------------------
	 */

    /**
     * Long identifier used to compare elements. Often, this is the same as {@link #getLongId()}
     * but some instances of elements may be considered the same even if their ids differ. In that case,
     * this method should be overwritten to return an id that can be used for comparison.
     * @return
     */
    protected long getCompareId() {
        return getLongId();
    }

    @Override
    public long getLongId() {
        return id;
    }

    public boolean hasId() {
        return !isTemporaryId(getLongId());
    }

    @Override
    public void setId(long id) {
        assert id > 0;
        this.id=id;
    }

    @Override
    public boolean isHidden() {
        return IDManager.VertexIDType.Hidden.is(id);
    }

    @Override
    public boolean isNew() {
        return ElementLifeCycle.isNew(it().getLifeCycle());
    }

    @Override
    public boolean isLoaded() {
        return ElementLifeCycle.isLoaded(it().getLifeCycle());
    }

    @Override
    public boolean isRemoved() {
        return ElementLifeCycle.isRemoved(it().getLifeCycle());
    }

}
