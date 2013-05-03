package com.thinkaurelius.titan.graphdb.transaction.indexcache;

import com.thinkaurelius.titan.core.TitanKey;
import com.thinkaurelius.titan.core.TitanProperty;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface IndexCache {

    public void add(TitanProperty property);

    public void remove(TitanProperty property);

    public Iterable<TitanProperty> get(Object value, TitanKey key);

}
