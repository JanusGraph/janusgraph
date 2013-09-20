package com.thinkaurelius.titan.graphdb.database.serialize;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.AttributeHandler;
import com.thinkaurelius.titan.core.TitanKey;
import com.thinkaurelius.titan.graphdb.database.serialize.AttributeHandling;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class DefaultAttributeHandling implements AttributeHandling {

    private final Map<Class,AttributeHandler> handlers;

    public DefaultAttributeHandling() {
        handlers = new HashMap<Class, AttributeHandler>(50);
    }

    @Override
    public <V> void registerClass(Class<V> datatype, AttributeHandler<V> handler) {
        Preconditions.checkNotNull(datatype);
        Preconditions.checkNotNull(handler);
        Preconditions.checkArgument(!handlers.containsKey(datatype),"DataType has already been registered: %s",datatype);
        handlers.put(datatype,handler);
    }

    @Override
    public <V> void verifyAttribute(Class<V> datatype, Object value) {
        Preconditions.checkNotNull(datatype);
        Preconditions.checkNotNull(value);
        AttributeHandler handler = handlers.get(datatype);
        if (handler!=null) handler.verifyAttribute(value);
    }

    @Override
    public <V> V convert(Class<V> datatype, Object value) {
        Preconditions.checkNotNull(datatype);
        Preconditions.checkNotNull(value);
        AttributeHandler handler = handlers.get(datatype);
        if (handler!=null) return (V)handler.convert(value);
        else return null;
    }
}
