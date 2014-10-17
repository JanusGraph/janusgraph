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
                key.getDataType().isInstance(value),"Value does not match data type: %s",value);
        this.key=key;
        this.value=value;
    }

    @Override
    public PropertyKey getPropertyKey() {
        return key;
    }

    @Override
    public TitanVertex getElement() {
        throw new UnsupportedOperationException();
    }

    @Override
    public <O> O getValue() {
        return (O)value;
    }

    @Override
    public FaunusRelationType getType() {
        return key;
    }

    protected Object otherValue() {
        return value;
    }



}
