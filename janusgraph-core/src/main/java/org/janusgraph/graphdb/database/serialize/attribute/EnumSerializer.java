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
import org.janusgraph.diskstorage.ScanBuffer;
import org.janusgraph.diskstorage.WriteBuffer;
import org.janusgraph.graphdb.database.idhandling.VariableLong;
import org.janusgraph.graphdb.database.serialize.OrderPreservingSerializer;

public class EnumSerializer<E extends Enum> implements OrderPreservingSerializer<E>  {

    private static final long serialVersionUID = 117423419862504186L;

    private final Class<E> datatype;
    private final IntegerSerializer ints = new IntegerSerializer();

    public EnumSerializer(Class<E> datatype) {
        Preconditions.checkArgument(datatype != null && datatype.isEnum());
        this.datatype = datatype;
    }

    private E getValue(long ordinal) {
        E[] values = datatype.getEnumConstants();
        Preconditions.checkArgument(ordinal>=0 && ordinal<values.length,"Invalid ordinal number (max %s): %s",values.length,ordinal);
        return values[(int)ordinal];
    }

    @Override
    public E read(ScanBuffer buffer) {
        return getValue(VariableLong.readPositive(buffer));
    }

    @Override
    public void write(WriteBuffer out, E object) {
        VariableLong.writePositive(out, object.ordinal());
    }

    @Override
    public E readByteOrder(ScanBuffer buffer) {
        return getValue(ints.readByteOrder(buffer));
    }


    @Override
    public void writeByteOrder(WriteBuffer buffer, E attribute) {
        ints.writeByteOrder(buffer,attribute.ordinal());
    }

}
