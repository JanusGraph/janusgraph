
package com.thinkaurelius.titan.graphdb.types;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.thinkaurelius.titan.core.TitanProperty;
import com.thinkaurelius.titan.graphdb.types.system.SystemKey;

import javax.annotation.Nullable;
import java.util.EnumMap;
import java.util.Set;

/**
* @author Matthias Broecheler (me@matthiasb.com)
*/
public class TypeDefinitionMap extends EnumMap<TypeDefinitionCategory,Object> {

    public TypeDefinitionMap() {
        super(TypeDefinitionCategory.class);
    }

    public TypeDefinitionMap(TypeDefinitionMap copy) {
        this();
        for (Entry<TypeDefinitionCategory,Object> entry : copy.entrySet()) {
            this.setValue(entry.getKey(),entry.getValue());
        }
    }

    public TypeDefinitionMap setValue(TypeDefinitionCategory type, Object value) {
        assert type  != null;
        assert value != null;
        assert type.verifyAttribute(value);
        super.put(type,value);
        return this;
    }

    public<O> O getValue(TypeDefinitionCategory type) {
        assert type != null;
        Object value = super.get(type);
        return (O) ((value == null) ? type.defaultValue(this) : value);
    }

    public<O> O getValue(TypeDefinitionCategory type, Class<O> clazz) {
        assert type != null;
        Object value = super.get(type);
        return (O) ((value == null) ? type.defaultValue(this) : value);
    }

    public void isValidKeyDefinition() {
        isValidDefinition(TypeDefinitionCategory.PROPERTY_KEY_DEFINITION_CATEGORIES);
    }

    public void isValidLabelDefinition() {
        isValidDefinition(TypeDefinitionCategory.EDGE_LABEL_DEFINITION_CATEGORIES);
    }

    private void isValidDefinition(Set<TypeDefinitionCategory> types) {
        Set<TypeDefinitionCategory> keys = this.keySet();
        for (TypeDefinitionCategory type : types) {
            assert keys.contains(type) : type + " not in " + this;
        }
        assert keys.size() == types.size() : keys.size() + " vs " + types.size() + " : " + keys.toString();
    }

}
