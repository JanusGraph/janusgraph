package org.janusgraph.graphdb.transaction.indexcache;

import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.TitanVertexProperty;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface IndexCache {

    public void add(TitanVertexProperty property);

    public void remove(TitanVertexProperty property);

    public Iterable<TitanVertexProperty> get(Object value, PropertyKey key);

}
