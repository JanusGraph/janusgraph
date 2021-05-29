// Copyright 2020 JanusGraph Authors
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

package org.janusgraph.graphdb.tinkerpop.io.binary;

import org.apache.commons.lang3.SerializationException;
import org.apache.tinkerpop.gremlin.structure.io.Buffer;
import org.apache.tinkerpop.gremlin.structure.io.binary.DataType;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryReader;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryWriter;
import org.apache.tinkerpop.gremlin.structure.io.binary.types.CustomTypeSerializer;

import java.io.IOException;

public abstract class JanusGraphTypeSerializer<T> implements CustomTypeSerializer<T> {

    private GraphBinaryType type;

    protected JanusGraphTypeSerializer(GraphBinaryType type) {
        this.type = type;
    }

    @Override
    public String getTypeName() {
        return type.getTypeName();
    }

    @Override
    public DataType getDataType() {
        return DataType.CUSTOM;
    }

    @Override
    public T read(Buffer buffer, GraphBinaryReader context) throws IOException {
        int customTypeInfo = buffer.readInt();
        if (customTypeInfo != type.getTypeId()) {
            throw new SerializationException(
                "Custom type info {" + customTypeInfo + "} doesn't match expect type info {" + type.getTypeId() + "}");
        }

        return readValue(buffer, context, true);
    }

    @Override
    public T readValue(Buffer buffer, GraphBinaryReader context, boolean nullable) throws IOException {
        if (nullable) {
            final byte valueFlag = buffer.readByte();
            if ((valueFlag & 1) == 1) {
                return null;
            }
        }
        return readNonNullableValue(buffer, context);
    }

    public abstract T readNonNullableValue(Buffer buffer, GraphBinaryReader context) throws IOException;

    @Override
    public void write(T value, Buffer buffer, GraphBinaryWriter context) throws IOException {
        buffer.writeInt(type.getTypeId());

        writeValue(value, buffer, context, true);
    }

    @Override
    public void writeValue(T value, Buffer buffer, GraphBinaryWriter context, boolean nullable) throws IOException {
        if (value == null) {
            if (!nullable) {
                throw new SerializationException("Unexpected null value when nullable is false");
            }

            context.writeValueFlagNull(buffer);
            return;
        }

        if (nullable) {
            context.writeValueFlagNone(buffer);
        }
        writeNonNullableValue(value, buffer, context);
    }

    protected abstract void writeNonNullableValue(T value, Buffer buffer, GraphBinaryWriter context) throws IOException;
}
