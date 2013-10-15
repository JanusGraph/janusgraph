package com.thinkaurelius.titan.graphdb.configuration;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.AttributeHandler;
import com.thinkaurelius.titan.core.AttributeSerializer;
import com.thinkaurelius.titan.graphdb.database.serialize.Serializer;
import com.thinkaurelius.titan.graphdb.database.serialize.SerializerInitialization;

/**
 * Helper class for registering data types with Titan
 *
 * @param <T>
 */
public class RegisteredAttributeClass<T> implements Comparable<RegisteredAttributeClass> {

    private final Class<T> type;
    private final AttributeHandler<T> serializer;
    private final int position;

    public RegisteredAttributeClass(Class<T> type, int position) {
        this(type, null, position);
    }

    public RegisteredAttributeClass(Class<T> type, AttributeHandler<T> serializer, int position) {
        Preconditions.checkArgument(position>=0,"Invalid position: %s",position);
        this.type = type;
        this.serializer = serializer;
        this.position = position;
    }

    private int getPosition() {
        return position+SerializerInitialization.RESERVED_ID_OFFSET;
    }

    void registerWith(Serializer s) {
        if (serializer == null) s.registerClass(type,getPosition());
        else if (serializer instanceof AttributeSerializer) s.registerClass(type, (AttributeSerializer)serializer, getPosition());
        else s.registerClass(type,serializer);
    }

    @Override
    public boolean equals(Object oth) {
        if (this == oth) return true;
        else if (!getClass().isInstance(oth)) return false;
        return type.equals(((RegisteredAttributeClass<?>) oth).type) || position == ((RegisteredAttributeClass) oth).position;
    }
    
    @Override
    public int hashCode() {
        int result = 17;
        result = 31 * result + position;
        if (null != type)
            result = 31 * result + type.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return type.toString() + "#" + position;
    }

    @Override
    public int compareTo(RegisteredAttributeClass registeredAttributeClass) {
        return position - registeredAttributeClass.position;
    }
}
