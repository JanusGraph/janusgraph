package com.thinkaurelius.titan.hadoop;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.graphdb.database.serialize.AttributeUtil;
import com.thinkaurelius.titan.graphdb.internal.ElementLifeCycle;
import com.thinkaurelius.titan.graphdb.internal.InternalVertex;
import com.thinkaurelius.titan.graphdb.relations.AbstractProperty;
import com.tinkerpop.blueprints.Direction;
import org.apache.commons.lang.builder.HashCodeBuilder;

import java.util.Collections;
import java.util.Set;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class SimpleFaunusProperty extends SimpleFaunusRelation implements FaunusProperty {

    private final FaunusPropertyKey key;
    private final Object value;

    public SimpleFaunusProperty(FaunusPropertyKey key, Object value) {
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
    public TitanVertex getVertex() {
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
