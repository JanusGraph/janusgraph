package com.thinkaurelius.titan.graphdb.configuration;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.attribute.AttributeHandler;
import com.thinkaurelius.titan.graphdb.database.serialize.Serializer;

/**
 * Helper class for registering data types with Titan
 *
 * @param <T>
 */
public class RegisteredAttributeClass<T> {

    private final Class<T> type;
    private final AttributeHandler<T> serializer;

    public RegisteredAttributeClass(Class<T> type, AttributeHandler<T> serializer) {
        Preconditions.checkNotNull(type);
        Preconditions.checkNotNull(serializer);
        this.type = type;
        this.serializer = serializer;
    }


    void registerWith(Serializer s) {
        s.registerClass(type,serializer);
    }

    @Override
    public boolean equals(Object oth) {
        if (this == oth) return true;
        else if (oth==null || !getClass().isInstance(oth)) return false;
        return type.equals(((RegisteredAttributeClass<?>) oth).type);
    }
    
    @Override
    public int hashCode() {
        return type.hashCode() + 110432;
    }

    @Override
    public String toString() {
        return type.toString();
    }
}
