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

package org.janusgraph.graphdb.configuration;

import com.google.common.base.Preconditions;
import org.janusgraph.core.attribute.AttributeSerializer;
import org.janusgraph.graphdb.database.serialize.Serializer;

/**
 * Helper class for registering data types with JanusGraph
 *
 * @param <T>
 */
public class RegisteredAttributeClass<T> {

    private final int position;
    private final Class<T> type;
    private final AttributeSerializer<T> serializer;

    public RegisteredAttributeClass(int position, Class<T> type, AttributeSerializer<T> serializer) {
        Preconditions.checkArgument(position>=0,"Position must be a positive integer, given: %s",position);
        this.position = position;
        this.type = Preconditions.checkNotNull(type);
        this.serializer = Preconditions.checkNotNull(serializer);
    }


    void registerWith(Serializer s) {
        s.registerClass(position,type,serializer);
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
