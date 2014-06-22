package com.thinkaurelius.titan.graphdb.types;

import com.thinkaurelius.titan.core.PropertyKey;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface MixedIndexType extends IndexType {

    public ParameterIndexField[] getFieldKeys();

    public ParameterIndexField getField(PropertyKey key);

    public String getStoreName();

}
