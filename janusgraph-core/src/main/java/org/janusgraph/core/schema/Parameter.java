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

package org.janusgraph.core.schema;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;

import java.util.Objects;

/**
 * Simple class to represent arbitrary parameters as key-value pairs.
 * Parameters are used in configuration and definitions.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class Parameter<V> {

    private final String key;
    private final V value;

    public Parameter(String key, V value) {
        Preconditions.checkArgument(StringUtils.isNotBlank(key),"Invalid key");
        this.key = key;
        this.value = value;
    }

    public static<V> Parameter<V> of(String key, V value) {
        return new Parameter(key,value);
    }

    public String key() {
        return key;
    }

    public V value() {
        return value;
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, value);
    }

    @Override
    public boolean equals(Object oth) {
        if (this == oth) return true;
        if (!getClass().isInstance(oth)) return false;
        Parameter other = (Parameter)oth;
        return key.equals(other.key) && Objects.equals(value, other.value);
    }

    @Override
    public String toString() {
        return key+"->"+ value;
    }

}
