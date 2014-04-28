
package com.thinkaurelius.titan.graphdb.types;

import com.google.common.base.Preconditions;

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

    public void isValidDefinition(Set<TypeDefinitionCategory> requiredTypes) {
        Set<TypeDefinitionCategory> keys = this.keySet();
        for (TypeDefinitionCategory type : requiredTypes) {
            Preconditions.checkArgument(keys.contains(type),"%s not in %s",type,this);
        }
        Preconditions.checkArgument(keys.size() == requiredTypes.size(),"Found irrelevant definitions in: %s",this);
    }

}
