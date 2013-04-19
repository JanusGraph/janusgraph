package com.thinkaurelius.titan.graphdb.internal;

import com.thinkaurelius.titan.core.TitanRelation;
import com.thinkaurelius.titan.core.TitanType;

/**
 * The abstract TitanRelation class defines the standard interface for edges used
 * in DOGMA.
 * It declares a set of operations and access methods that all edge implementations
 * provide.
 *
 * @author Matthias Broecheler (me@matthiasb.com);
 */
public interface InternalRelation extends TitanRelation, InternalElement {

    public InternalRelation it();

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

    public boolean isHidden();

    public <O> O getProperty(TitanType type);

    public <O> O getPropertyDirect(TitanType type);

    public void setPropertyDirect(TitanType type, Object value);

    public Iterable<TitanType> getPropertyKeysDirect();

    public <O> O  removePropertyDirect(TitanType type);


}
