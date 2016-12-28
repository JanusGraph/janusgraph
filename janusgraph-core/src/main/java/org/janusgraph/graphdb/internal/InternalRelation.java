package com.thinkaurelius.titan.graphdb.internal;

import com.thinkaurelius.titan.core.PropertyKey;
import com.thinkaurelius.titan.core.RelationType;
import com.thinkaurelius.titan.core.TitanRelation;

/**
 * Internal Relation interface adding methods that should only be used by Titan.
 *
 * The "direct" qualifier in the method names indicates that the corresponding action is executed on this relation
 * object and not migrated to a different transactional context. It also means that access returns the "raw" value of
 * what is stored on this relation
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface InternalRelation extends TitanRelation, InternalElement {

    /**
     * Returns this relation in the current transactional context
     *
     * @return
     */
    @Override
    public InternalRelation it();

    /**
     * Returns the vertex at the given position (0=OUT, 1=IN) of this relation
     * @param pos
     * @return
     */
    public InternalVertex getVertex(int pos);

    /**
     * Number of vertices on this relation.
     *
     * @return
     */
    public int getArity();

    /**
     * Number of vertices on this relation that are aware of its existence. This number will
     * differ from {@link #getArity()}
     *
     */
    public int getLen();



    public <O> O getValueDirect(PropertyKey key);

    public void setPropertyDirect(PropertyKey key, Object value);

    public Iterable<PropertyKey> getPropertyKeysDirect();

    public <O> O removePropertyDirect(PropertyKey key);

}
