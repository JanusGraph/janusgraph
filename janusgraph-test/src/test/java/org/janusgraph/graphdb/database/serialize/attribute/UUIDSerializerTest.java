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

import org.janusgraph.diskstorage.util.ReadArrayBuffer;
import org.janusgraph.diskstorage.util.WriteByteBuffer;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author Bryn Cooke (bryn.cooke@datastax.com)
 */
public class UUIDSerializerTest {

    @Test
    public void testRoundTrip() {
        //Write the UUID
        UUIDSerializer serializer = new UUIDSerializer();
        UUID uuid1 = UUID.randomUUID();
        WriteByteBuffer buffer = new WriteByteBuffer();
        serializer.write(buffer, uuid1);

        //And read it in again
        ReadArrayBuffer readBuffer = new ReadArrayBuffer(buffer.getStaticBuffer().getBytes(0, 16));
        UUID uuid2 = serializer.read(readBuffer);

        assertEquals(uuid1, uuid2);
    }

    @Test
    public void testRoundTripByteOrder() {
        //Write the UUID
        UUIDSerializer serializer = new UUIDSerializer();
        UUID uuid1 = UUID.randomUUID();
        WriteByteBuffer buffer = new WriteByteBuffer();
        serializer.writeByteOrder(buffer, uuid1);

        //And read it in again
        ReadArrayBuffer readBuffer = new ReadArrayBuffer(buffer.getStaticBuffer().getBytes(0, 16));
        UUID uuid2 = serializer.readByteOrder(readBuffer);

        assertEquals(uuid1, uuid2);
    }

    @Test
    public void testConvert() {
        //Write the UUID
        UUIDSerializer serializer = new UUIDSerializer();
        UUID parsed = serializer.convert("d320e751-3a9c-48a8-88f5-2c8b455baa5f");
        assertEquals(UUID.fromString("d320e751-3a9c-48a8-88f5-2c8b455baa5f"), parsed);
    }
}
