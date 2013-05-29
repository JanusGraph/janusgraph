package com.thinkaurelius.titan.graphdb.internal;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Longs;
import com.thinkaurelius.titan.core.TitanEdge;
import com.thinkaurelius.titan.core.TitanElement;
import com.thinkaurelius.titan.core.TitanVertex;

/**
 * AbstractElement is the base class for all elements in Titan.
 * It is defined and uniquely identified by its id.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public abstract class AbstractElement implements InternalElement {

    private long id;

    public AbstractElement(final long id) {
        Preconditions.checkArgument(id != 0);
        this.id = id;
    }

    public static final boolean isTemporaryId(long elementId) {
        return elementId<0;
    }


    @Override
    public int hashCode() {
        return Longs.hashCode(getID());
    }

    @Override
    public boolean equals(Object other) {
        if (this==other) {
            return true;
        } else if (other==null) {
            return false;
        } else if (this instanceof TitanVertex && other instanceof TitanVertex) {
            return getID() == ((TitanVertex)other).getID();
        } else if (this instanceof TitanEdge && other instanceof TitanEdge) {
            return getID() == ((TitanEdge)other).getID();
        } else {
            return false;
        }
    }


    @Override
    public int compareTo(TitanElement titanElement) {
        return Longs.compare(getID(),titanElement.getID());
    }

    @Override
    public InternalVertex clone() throws CloneNotSupportedException {
        throw new CloneNotSupportedException();
    }

    /* ---------------------------------------------------------------
	 * ID and LifeCycle methods
	 * ---------------------------------------------------------------
	 */

    @Override
    public long getID() {
        return id;
    }

    public boolean hasId() {
        return !isTemporaryId(getID());
    }

    @Override
    public void setID(long id) {
        Preconditions.checkArgument(isTemporaryId(this.id),"Element has already been assigned an id");
        Preconditions.checkArgument(id>0);
        this.id=id;
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
