package com.thinkaurelius.titan.graphdb.types;

import com.thinkaurelius.titan.core.Cardinality;
import com.thinkaurelius.titan.core.schema.ConsistencyModifier;
import com.thinkaurelius.titan.core.schema.SchemaStatus;

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
