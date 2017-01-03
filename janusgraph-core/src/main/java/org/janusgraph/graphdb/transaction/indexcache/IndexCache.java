package org.janusgraph.graphdb.transaction.indexcache;

import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.JanusVertexProperty;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface IndexCache {

    public void add(JanusVertexProperty property);

    public void remove(JanusVertexProperty property);

    public Iterable<JanusVertexProperty> get(Object value, PropertyKey key);

}
