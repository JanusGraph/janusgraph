package com.thinkaurelius.titan.graphdb.types;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.Cardinality;
import com.thinkaurelius.titan.core.ConsistencyModifier;
import com.thinkaurelius.titan.core.TitanKey;
import com.thinkaurelius.titan.graphdb.internal.ElementCategory;
import com.thinkaurelius.titan.graphdb.query.condition.And;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;
import org.apache.commons.lang.builder.HashCodeBuilder;

/**
* @author Matthias Broecheler (me@matthiasb.com)
*/
public interface InternalIndexType extends IndexType {

    public long getID();

    public IndexField[] getFields();

    public boolean isEnabled();

    /*
     * single == unique,
     */
    public Cardinality getCardinality();

    public ConsistencyModifier getConsistencyModifier();

}
