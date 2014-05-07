package com.thinkaurelius.titan.graphdb.internal;

import com.google.common.primitives.Longs;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.graphdb.idmanagement.IDManager;

/**
 * AbstractElement is the base class for all elements in Titan.
 * It is defined and uniquely identified by its id.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public abstract class AbstractElement implements InternalElement {

    private long id;

    public AbstractElement(long id) {
        assert id != 0;
        this.id = id;
    }

    public static boolean isTemporaryId(long elementId) {
        return elementId < 0;
    }


    @Override
    public int hashCode() {
        return Longs.hashCode(getID());
    }

    @Override
    public boolean equals(Object other) {
        if (other == null)
            return false;

        if (this == other)
            return true;

        try {
            if (id != ((TitanElement)other).getID()) return false;
        } catch (ClassCastException e) {
            return false;
        }

        if (this instanceof TitanVertex && other instanceof TitanVertex)
            return true;

        if (this instanceof TitanRelation && other instanceof TitanRelation)
            return true;

        return false;
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
