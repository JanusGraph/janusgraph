package com.thinkaurelius.titan.core.attribute;

import java.io.Serializable;
import java.nio.ByteBuffer;

/**
 * Allows custom serializer definitions for attribute values.
 * 
 * For most data types used with property types, using the default serializer when registering the type with the
 * graph database will be sufficient and efficient in practice. However, for certain data types, it can be more
 * efficient to provide custom serializers implementing this interface.
 * Such serializers are registered with {@link com.thinkaurelius.titan.configuration.GraphDatabaseConfiguration#registerClass(Class, com.thinkaurelius.titan.core.attribute.AttributeSerializer)}.
 * 
 * Note that a custom AttributeSerializer must be registered for all RangeAttribute subclasses.
 * 
 * @author	Matthias Br&ouml;cheler (me@matthiasb.com);
 * 
 * 
 * @see com.thinkaurelius.titan.core.PropertyIndex
 * @param <V> Type of the attribute associated with the AttributeSerializer
 */
public interface AttributeSerializer<V> extends Serializable {

	public V read(ByteBuffer buffer);

	public void writeObjectData(ByteBuffer buffer, V attribute);
	
}
