package org.janusgraph.graphdb.types;

import org.janusgraph.core.Cardinality;
import org.janusgraph.core.schema.ConsistencyModifier;
import org.janusgraph.core.schema.SchemaStatus;

/**
* @author Matthias Broecheler (me@matthiasb.com)
*/
public interface CompositeIndexType extends IndexType {

    public long getID();

    public IndexField[] getFieldKeys();

    public SchemaStatus getStatus();

    /*
     * single == unique,
     */
    public Cardinality getCardinality();

    public ConsistencyModifier getConsistencyModifier();
}
