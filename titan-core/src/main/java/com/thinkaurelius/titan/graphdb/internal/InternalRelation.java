package com.thinkaurelius.titan.graphdb.internal;

import com.thinkaurelius.titan.core.RelationType;
import com.thinkaurelius.titan.core.TitanRelation;

/**
 * Internal Relation interface adding methods that should only be used by Titan
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface InternalRelation extends TitanRelation, InternalElement {

    public InternalRelation it();

    /**
     * Returns the vertex at the given position (0=OUT, 1=IN) of this relation
     * @param pos
     * @return
     */
    public InternalVertex getVertex(int pos);

    /**
     * Number of vertices on this relation
     * @return
     */
    public int getArity();

    /**
     * Number of vertices on this relation that are aware of its existence
     * @see com.thinkaurelius.titan.core.TitanEdge#isUnidirected()
     */
    public int getLen();

    public <O> O getProperty(RelationType type);

    public <O> O getPropertyDirect(RelationType type);

    public void setPropertyDirect(RelationType type, Object value);

    public Iterable<RelationType> getPropertyKeysDirect();

    public <O> O  removePropertyDirect(RelationType type);


}
