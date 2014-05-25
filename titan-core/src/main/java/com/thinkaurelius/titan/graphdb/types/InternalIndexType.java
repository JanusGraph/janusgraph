package com.thinkaurelius.titan.graphdb.types;

import com.thinkaurelius.titan.core.Cardinality;
import com.thinkaurelius.titan.core.schema.ConsistencyModifier;

/**
* @author Matthias Broecheler (me@matthiasb.com)
*/
public interface InternalIndexType extends IndexType {

    public long getID();

    public IndexField[] getFieldKeys();

    public SchemaStatus getStatus();

    /*
     * single == unique,
     */
    public Cardinality getCardinality();

    public ConsistencyModifier getConsistencyModifier();

}
