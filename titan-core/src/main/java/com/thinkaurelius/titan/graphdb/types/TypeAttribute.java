package com.thinkaurelius.titan.graphdb.types;

import com.google.common.base.Function;
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
        assert type  != null;
        assert value != null;
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
        assert definition != null;
        Set<TypeAttributeType> keys = definition.keySet();
        for (TypeAttributeType type : types) {
           assert keys.contains(type);
        }
        assert keys.size() == types.size();
    }

    public static class Map extends EnumMap<TypeAttributeType,Object> {

        public Map() {
            super(TypeAttributeType.class);
        }

        public void add(TypeAttribute attribute) {
            setValue(attribute.getType(),attribute.getValue());
        }

        public Map setValue(TypeAttributeType type, Object value) {
            assert type  != null;
            assert value != null;
            assert type.verifyAttribute(value);
            super.put(type,value);
            return this;
        }

        public<O> O getValue(TypeAttributeType type) {
            assert type != null;
            Object value = super.get(type);
            return (O) ((value == null) ? type.defaultValue(this) : value);
        }

        public<O> O getValue(TypeAttributeType type, Class<O> clazz) {
            assert type != null;
            Object value = super.get(type);
            return (O) ((value == null) ? type.defaultValue(this) : value);
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
