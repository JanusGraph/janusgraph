package com.thinkaurelius.titan.graphdb.internal;

import com.thinkaurelius.titan.core.TitanRelation;
import com.thinkaurelius.titan.core.TitanType;
import com.thinkaurelius.titan.graphdb.internal.InternalVertex;

/**
 * The abstract TitanRelation class defines the standard interface for edges used
 * in DOGMA.
 * It declares a set of operations and access methods that all edge implementations
 * provide.
 *
 * @author Matthias Broecheler (me@matthiasb.com);
 */
public interface InternalRelation extends TitanRelation, InternalElement {

    public InternalVertex getVertex(int pos);

    public int getArity();

    public boolean isHidden();

    public Object getPropertyDirect(TitanType type);

    public void setPropertyDirect(TitanType type, Object value);

    public Iterable<TitanType> getPropertyKeysDirect();

    public Object removePropertyDirect(TitanType type);


}
