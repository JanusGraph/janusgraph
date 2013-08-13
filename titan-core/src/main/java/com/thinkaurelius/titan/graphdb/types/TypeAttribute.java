package com.thinkaurelius.titan.graphdb.types;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;

import javax.annotation.Nullable;
import java.util.EnumMap;
import java.util.Set;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class TypeAttribute {

    private TypeAttributeType type;
    private Object value;

    public TypeAttribute() {}

    public TypeAttribute(TypeAttributeType type, Object value) {
        Preconditions.checkNotNull(type);
        Preconditions.checkNotNull(value);
        this.type = type;
        this.value = value;
    }


    public Object getValue() {
        return value;
    }

    public TypeAttributeType getType() {
        return type;
    }

    public static void isValidKeyDefinition(Map definition) {
        isValidDefinition(definition,TypeAttributeType.PROPERTY_KEY_TYPES);
    }

    public static void isValidLabelDefinition(Map definition) {
        isValidDefinition(definition,TypeAttributeType.EDGE_LABEL_TYPES);
    }

    private static void isValidDefinition(Map definition, Set<TypeAttributeType> types) {
        Preconditions.checkNotNull(definition);
        Set<TypeAttributeType> keys = definition.keySet();
        for (TypeAttributeType type : types) {
            Preconditions.checkArgument(keys.contains(type),"Missing type in definition: %s",type);
        }
        Preconditions.checkArgument(keys.size()==types.size(),"Extra types found in definition: %s",keys);
    }

    public static class Map extends EnumMap<TypeAttributeType,Object> {

        public Map() {
            super(TypeAttributeType.class);
        }

        public void add(TypeAttribute attribute) {
            setValue(attribute.getType(),attribute.getValue());
        }

        public Map setValue(TypeAttributeType type, Object value) {
            Preconditions.checkNotNull(type);
            Preconditions.checkNotNull(value);
            Preconditions.checkArgument(type.verifyAttribute(value));
            super.put(type,value);
            return this;
        }

        public<O> O getValue(TypeAttributeType type) {
            Preconditions.checkNotNull(type);
            Object value = super.get(type);
            if (value==null) value = type.defaultValue(this);
            return (O)value;
        }

        public<O> O getValue(TypeAttributeType type, Class<O> clazz) {
            Preconditions.checkNotNull(type);
            Object value = super.get(type);
            if (value==null) value = type.defaultValue(this);
            return (O)value;
        }

        public Iterable<TypeAttribute> getAttributes() {
            return Iterables.transform(super.entrySet(),new Function<Entry<TypeAttributeType,Object>,TypeAttribute>(){
                @Nullable
                @Override
                public TypeAttribute apply(@Nullable Entry<TypeAttributeType, Object> entry) {
                    return new TypeAttribute(entry.getKey(),entry.getValue());
                }
            });
        }

    }

}
