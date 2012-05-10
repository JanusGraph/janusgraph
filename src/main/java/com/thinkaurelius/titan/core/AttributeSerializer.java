package com.thinkaurelius.titan.core;

import java.io.Serializable;
import java.nio.ByteBuffer;

/**
 * Allows custom serializer definitions for attribute values.
 * 
 * For most data types used with property types, using the default serializer when registering the type with the
 * graph database will be sufficient and efficient in practice. However, for certain data types, it can be more
 * efficient to provide custom serializers implementing this interface.
 * Such serializers are registered with {@link com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration#registerClass(Class, AttributeSerializer)}.
 * 
 *
 * @author	Matthias Br&ouml;cheler (me@matthiasb.com);
 * 
 *
 * @param <V> Type of the attribute associated with the AttributeSerializer
 */
public interface AttributeSerializer<V> extends Serializable {

	public V read(ByteBuffer buffer);

	public void writeObjectData(ByteBuffer buffer, V attribute);
	
}
