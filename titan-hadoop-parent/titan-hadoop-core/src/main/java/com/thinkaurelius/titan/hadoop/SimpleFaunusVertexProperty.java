package com.thinkaurelius.titan.hadoop;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.graphdb.database.serialize.AttributeUtil;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class SimpleFaunusVertexProperty extends SimpleFaunusRelation implements FaunusVertexProperty {

    private final FaunusPropertyKey key;
    private final Object value;

    public SimpleFaunusVertexProperty(FaunusPropertyKey key, Object value) {
        Preconditions.checkArgument(key!=null);
        Preconditions.checkArgument(value != null);
        Preconditions.checkArgument(AttributeUtil.hasGenericDataType(key) ||
                key.dataType().isInstance(value),"Value does not match data type: %s",value);
        this.key=key;
        this.value=value;
    }

    @Override
    public TitanVertex element() {
        throw new UnsupportedOperationException();
    }

    @Override
    public <O> O getValue() {
        return (O)value;
    }

    protected Object otherValue() {
        return value;
    }



}
