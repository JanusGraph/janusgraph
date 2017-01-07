package org.janusgraph.graphdb.transaction.indexcache;

import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.JanusGraphVertexProperty;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface IndexCache {

    public void add(JanusGraphVertexProperty property);

    public void remove(JanusGraphVertexProperty property);

    public Iterable<JanusGraphVertexProperty> get(Object value, PropertyKey key);

}
