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

import org.apache.tinkerpop.gremlin.structure.io.Buffer;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryReader;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryWriter;
import org.janusgraph.core.attribute.Geoshape;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class GeoshapeGraphBinarySerializer extends JanusGraphTypeSerializer<Geoshape> {
    public GeoshapeGraphBinarySerializer() {
        super(GraphBinaryType.Geoshape);
    }

    private static class BufferInputStream extends InputStream {

        private Buffer buffer;

        public BufferInputStream(Buffer buffer) {
            this.buffer = buffer;
        }

        @Override
        public int read() {
            return buffer.readInt();
        }
    }

    @Override
    public Geoshape readNonNullableValue(Buffer buffer, GraphBinaryReader context) throws IOException {
        BufferInputStream bufferOutputStream = new BufferInputStream(buffer);
        return Geoshape.GeoshapeBinarySerializer.read(bufferOutputStream);
    }

    private static class BufferOutputStream extends OutputStream {

        private Buffer buffer;

        public BufferOutputStream(Buffer buffer) {
            this.buffer = buffer;
        }

        @Override
        public void write(int i) {
            buffer.writeInt(i);
        }
    }

    @Override
    public void writeNonNullableValue(Geoshape geoshape, Buffer buffer, GraphBinaryWriter context) throws IOException {
        BufferOutputStream bufferOutputStream = new BufferOutputStream(buffer);
        Geoshape.GeoshapeBinarySerializer.write(bufferOutputStream, geoshape);
    }
}
