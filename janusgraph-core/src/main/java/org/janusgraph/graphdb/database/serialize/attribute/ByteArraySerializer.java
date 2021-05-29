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

import org.janusgraph.core.attribute.AttributeSerializer;
import org.janusgraph.diskstorage.ScanBuffer;
import org.janusgraph.diskstorage.WriteBuffer;

import java.lang.reflect.Array;

public class ByteArraySerializer extends ArraySerializer implements AttributeSerializer<byte[]> {

    @Override
    public byte[] convert(Object value) {
        return convertInternal(value, byte.class, Byte.class);
    }

    @Override
    protected Object getArray(int length) {
        return new byte[length];
    }

    @Override
    protected void setArray(Object array, int pos, Object value) {
        Array.setByte(array,pos,((Byte)value));
    }

    //############### Serialization ###################

    @Override
    public byte[] read(ScanBuffer buffer) {
        int length = getLength(buffer);
        if (length<0) return null;
        return buffer.getBytes(length);
    }

    @Override
    public void write(WriteBuffer buffer, byte[] attribute) {
        writeLength(buffer,attribute);
        if (attribute!=null) for (byte anAttribute : attribute) buffer.putByte(anAttribute);
    }
}
