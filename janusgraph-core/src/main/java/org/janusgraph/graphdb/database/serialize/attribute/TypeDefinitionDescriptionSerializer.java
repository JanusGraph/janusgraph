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

package org.janusgraph.graphdb.database.serialize.attribute;

import com.google.common.base.Preconditions;
import org.janusgraph.core.attribute.AttributeSerializer;
import org.janusgraph.diskstorage.ScanBuffer;
import org.janusgraph.diskstorage.WriteBuffer;
import org.janusgraph.graphdb.database.serialize.DataOutput;
import org.janusgraph.graphdb.database.serialize.Serializer;
import org.janusgraph.graphdb.database.serialize.SerializerInjected;
import org.janusgraph.graphdb.types.TypeDefinitionCategory;
import org.janusgraph.graphdb.types.TypeDefinitionDescription;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class TypeDefinitionDescriptionSerializer implements AttributeSerializer<TypeDefinitionDescription>, SerializerInjected {

    private Serializer serializer;

    @Override
    public TypeDefinitionDescription read(ScanBuffer buffer) {
        TypeDefinitionCategory defCategory = serializer.readObjectNotNull(buffer, TypeDefinitionCategory.class);
        Object modifier = serializer.readClassAndObject(buffer);
        return new TypeDefinitionDescription(defCategory,modifier);
    }

    @Override
    public void write(WriteBuffer buffer, TypeDefinitionDescription attribute) {
        DataOutput out = (DataOutput)buffer;
        out.writeObjectNotNull(attribute.getCategory());
        out.writeClassAndObject(attribute.getModifier());
    }

    @Override
    public void setSerializer(Serializer serializer) {
        this.serializer = Preconditions.checkNotNull(serializer);
    }
}
