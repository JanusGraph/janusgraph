package com.thinkaurelius.titan.graphdb.internal;

import com.google.common.primitives.Longs;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.graphdb.idmanagement.IDManager;
import com.thinkaurelius.titan.graphdb.relations.RelationIdentifier;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;

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
        return Long.valueOf(getCompareId()).hashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (other==null)
            return false;

        if (this==other)
            return true;
        if (!((this instanceof Vertex && other instanceof Vertex) ||
                (this instanceof Edge && other instanceof Edge) ||
                (this instanceof VertexProperty && other instanceof VertexProperty)))
            return false;
        //Same type => they are the same if they have identical ids.
        if (other instanceof AbstractElement) {
            return getCompareId()==((AbstractElement)other).getCompareId();
        } else if (other instanceof TitanElement) {
            return ((TitanElement) other).hasId() && getCompareId()==((TitanElement)other).longId();
        } else if (other instanceof Element) {
            Object otherId = ((Element)other).id();
            if (otherId instanceof RelationIdentifier) return ((RelationIdentifier) otherId).getRelationId()==getCompareId();
            else return otherId.equals(getCompareId());
        } else return false;
    }


    @Override
    public int compareTo(TitanElement other) {
        return compare(this,other);
    }

    public static int compare(TitanElement e1, TitanElement e2) {
        long e1id = (e1 instanceof AbstractElement)?((AbstractElement)e1).getCompareId():e1.longId();
        long e2id = (e2 instanceof AbstractElement)?((AbstractElement)e2).getCompareId():e2.longId();
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
     * Long identifier used to compare elements. Often, this is the same as {@link #longId()}
     * but some instances of elements may be considered the same even if their ids differ. In that case,
     * this method should be overwritten to return an id that can be used for comparison.
     * @return
     */
    protected long getCompareId() {
        return longId();
    }

    @Override
    public long longId() {
        return id;
    }

    public boolean hasId() {
        return !isTemporaryId(longId());
    }

    @Override
    public void setId(long id) {
        assert id > 0;
        this.id=id;
    }

    @Override
    public boolean isInvisible() {
        return IDManager.VertexIDType.Invisible.is(id);
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
