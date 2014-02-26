package com.thinkaurelius.titan.graphdb.types;

import com.thinkaurelius.titan.core.TitanKey;
import com.thinkaurelius.titan.graphdb.internal.ElementCategory;
import com.thinkaurelius.titan.graphdb.query.condition.And;

import java.util.Set;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface IndexType {

    public ElementCategory getElement();

    public IndexField[] getFields();

    public IndexField getField(TitanKey key);

    public boolean indexesKey(TitanKey key);

    public boolean isInternalIndex();

    public boolean isExternalIndex();


    //TODO: Add in the future
    //public And getCondition();


}
