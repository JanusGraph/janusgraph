package com.thinkaurelius.titan.graphdb.types;

import com.thinkaurelius.titan.core.TitanGraphIndex;
import com.thinkaurelius.titan.core.TitanKey;
import com.thinkaurelius.titan.graphdb.internal.ElementCategory;
import com.thinkaurelius.titan.graphdb.query.condition.And;

import java.util.Set;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface IndexType {

    public ElementCategory getElement();

    public IndexField[] getFieldKeys();

    public IndexField getField(TitanKey key);

    public boolean indexesKey(TitanKey key);

    public boolean isInternalIndex();

    public boolean isExternalIndex();

    public String getBackingIndexName();

    public String getName();

    /**
     * Resets the internal caches used to speed up lookups on this index.
     * This is needed when the index gets modified in {@link com.thinkaurelius.titan.graphdb.database.management.ManagementSystem}.
     */
    public void resetCache();

    //TODO: Add in the future
    //public And getCondition();


}
