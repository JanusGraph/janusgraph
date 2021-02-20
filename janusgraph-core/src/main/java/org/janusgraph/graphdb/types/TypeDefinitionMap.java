// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


package org.janusgraph.graphdb.types;

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

    // note: special case takes into account that only one modifier is present on each type modifier vertex
    public void isValidTypeModifierDefinition(Set<TypeDefinitionCategory> legalTypes) {
        Preconditions.checkArgument(1 == this.size(), "exactly one type modifier is expected");
        for (TypeDefinitionCategory type : this.keySet()) {
            Preconditions.checkArgument(legalTypes.contains(type), "%s not legal here", type);
        }
    }
}
