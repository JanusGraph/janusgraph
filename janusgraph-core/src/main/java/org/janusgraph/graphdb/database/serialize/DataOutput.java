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

package org.janusgraph.graphdb.database.serialize;

import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.WriteBuffer;

public interface DataOutput extends WriteBuffer {

    @Override
    public DataOutput putLong(long val);

    @Override
    public DataOutput putInt(int val);

    @Override
    public DataOutput putShort(short val);

    @Override
    public DataOutput putByte(byte val);

    @Override
    public DataOutput putBytes(byte[] val);

    @Override
    public DataOutput putBytes(StaticBuffer val);

    @Override
    public DataOutput putChar(char val);

    @Override
    public DataOutput putFloat(float val);

    @Override
    public DataOutput putDouble(double val);

    public DataOutput writeObject(Object object, Class<?> type);

    public DataOutput writeObjectByteOrder(Object object, Class<?> type);

    public DataOutput writeObjectNotNull(Object object);

    public DataOutput writeClassAndObject(Object object);

}
