package com.thinkaurelius.titan.graphdb.types;

import com.thinkaurelius.titan.core.Parameter;
import com.thinkaurelius.titan.core.TitanKey;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface ExternalIndexType extends IndexType {

    public ParameterIndexField[] getFields();

    public ParameterIndexField getField(TitanKey key);

    public String getIndexName();

}
