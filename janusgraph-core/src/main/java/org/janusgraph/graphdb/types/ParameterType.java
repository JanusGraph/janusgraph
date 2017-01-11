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
import org.janusgraph.core.schema.Parameter;
import org.apache.commons.lang.StringUtils;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public enum ParameterType {

    MAPPING("mapping"), INDEX_POSITION("index-pos"), MAPPED_NAME("mapped-name"), STATUS("status");

    private final String name;

    private ParameterType(String name) {
        Preconditions.checkArgument(StringUtils.isNotBlank(name));
        this.name=name;
    }

    @Override
    public String toString() {
        return name;
    }

    public String getName() {
        return name;
    }

    public<V> V findParameter(Parameter[] parameters, V defaultValue) {
        V result = null;
        for (Parameter p : parameters) {
            if (p.key().equalsIgnoreCase(name)) {
                Object value = p.value();
                Preconditions.checkArgument(value!=null,"Invalid mapping specified: %s",value);
                Preconditions.checkArgument(result==null,"Multiple mappings specified");
                result = (V)value;
            }
        }
        if (result==null) return defaultValue;
        return result;
    }

    public boolean hasParameter(Parameter[] parameters) {
        return findParameter(parameters,null)!=null;
    }

    public<V> Parameter<V> getParameter(V value) {
        return new Parameter<V>(name,value);
    }



}

