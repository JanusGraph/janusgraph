package com.thinkaurelius.titan.graphdb.types;

import com.thinkaurelius.titan.core.PropertyKey;
import com.thinkaurelius.titan.core.schema.TitanSchemaType;
import com.thinkaurelius.titan.graphdb.internal.ElementCategory;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface IndexType {

    public ElementCategory getElement();

    public IndexField[] getFieldKeys();

    public IndexField getField(PropertyKey key);

    public boolean indexesKey(PropertyKey key);

    public boolean isCompositeIndex();

    public boolean isMixedIndex();

    public boolean hasSchemaTypeConstraint();

    public TitanSchemaType getSchemaTypeConstraint();

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
