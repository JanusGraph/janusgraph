package org.janusgraph.graphdb.types;

import org.janusgraph.core.PropertyKey;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface MixedIndexType extends IndexType {

    public ParameterIndexField[] getFieldKeys();

    public ParameterIndexField getField(PropertyKey key);

    public String getStoreName();

}
