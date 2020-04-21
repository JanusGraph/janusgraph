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
import org.janusgraph.core.PropertyKey;

import java.util.Objects;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class IndexField {

    private final PropertyKey key;

    IndexField(PropertyKey key) {
        this.key = Preconditions.checkNotNull(key);
    }

    public PropertyKey getFieldKey() {
        return key;
    }

    public static IndexField of(PropertyKey key) {
        return new IndexField(key);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(key);
    }

    @Override
    public boolean equals(Object oth) {
        if (this==oth) return true;
        else if (oth==null || !getClass().isInstance(oth)) return false;
        IndexField other = (IndexField)oth;
        if (key==null) return key==other.key;
        else return key.equals(other.key);
    }

    @Override
    public String toString() {
        return "["+key.name()+"]";
    }

}
