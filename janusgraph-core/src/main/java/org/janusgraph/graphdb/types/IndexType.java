package org.janusgraph.graphdb.types;

import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.JanusGraphSchemaType;
import org.janusgraph.graphdb.internal.ElementCategory;

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

    public JanusGraphSchemaType getSchemaTypeConstraint();

    public String getBackingIndexName();

    public String getName();

    /**
     * Resets the internal caches used to speed up lookups on this index.
     * This is needed when the index gets modified in {@link org.janusgraph.graphdb.database.management.ManagementSystem}.
     */
    public void resetCache();

    //TODO: Add in the future
    //public And getCondition();


}
