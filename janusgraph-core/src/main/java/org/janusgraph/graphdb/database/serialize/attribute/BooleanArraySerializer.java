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

public class BooleanArraySerializer extends ArraySerializer implements AttributeSerializer<boolean[]> {

    @Override
    public boolean[] convert(Object value) {
        return convertInternal(value, boolean.class, Boolean.class);
    }

    @Override
    protected Object getArray(int length) {
        return new boolean[length];
    }

    @Override
    protected void setArray(Object array, int pos, Object value) {
        Array.setBoolean(array, pos, ((Boolean) value));
    }

    //############### Serialization ###################

    @Override
    public boolean[] read(ScanBuffer buffer) {
        int length = getLength(buffer);
        if (length<0) return null;
        boolean[] result = new boolean[length];
        int b = 0;
        for (int i = 0; i < length; i++) {
            int offset = i%8;
            if (offset==0) {
                b= 0xFF & buffer.getByte();
            }
            result[i]=BooleanSerializer.decode((byte)((b>>>(7-offset))&1));
        }
        return result;
    }

    @Override
    public void write(WriteBuffer buffer, boolean[] attribute) {
        writeLength(buffer,attribute);
        if (attribute==null) return;
        byte b = 0;
        int i = 0;
        for (; i < attribute.length; i++) {
            b = (byte)( ((int)b<<1) | BooleanSerializer.encode(attribute[i]));
            if ((i+1)%8 == 0) {
                buffer.putByte(b);
                b=0;
            }
        }
        if (i%8!=0) buffer.putByte((byte)(b<<(8-(i%8))));
    }
}
