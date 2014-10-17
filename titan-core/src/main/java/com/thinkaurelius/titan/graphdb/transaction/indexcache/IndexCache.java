package com.thinkaurelius.titan.graphdb.transaction.indexcache;

import com.thinkaurelius.titan.core.PropertyKey;
import com.thinkaurelius.titan.core.TitanVertexProperty;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface IndexCache {

    public void add(TitanVertexProperty property);

    public void remove(TitanVertexProperty property);

    public Iterable<TitanVertexProperty> get(Object value, PropertyKey key);

}
