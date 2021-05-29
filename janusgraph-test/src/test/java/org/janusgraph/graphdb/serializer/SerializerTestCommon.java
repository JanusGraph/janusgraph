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

package org.janusgraph.graphdb.serializer;

import org.janusgraph.diskstorage.ReadBuffer;
import org.janusgraph.graphdb.database.serialize.DataOutput;
import org.janusgraph.graphdb.database.serialize.Serializer;
import org.janusgraph.graphdb.database.serialize.StandardSerializer;
import org.janusgraph.graphdb.serializer.attributes.TClass1;
import org.janusgraph.graphdb.serializer.attributes.TClass2;
import org.janusgraph.graphdb.serializer.attributes.TEnum;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class SerializerTestCommon {

    private static final Logger log =
            LoggerFactory.getLogger(SerializerTestCommon.class);

    protected Serializer serialize;
    protected boolean printStats;

    @BeforeEach
    public void setUp() {
        serialize = new StandardSerializer();
        printStats = false;
    }

    @AfterEach
    public void tearDown() throws Exception {
        serialize.close();
    }

    protected void objectWriteRead() {
        TClass1 t1 = new TClass1(3245234223423433123L,0.333f);
        TClass2 t2 = new TClass2("This is a test",4234234);
        TEnum t3 = TEnum.THREE;
        TEnum t4 = TEnum.TWO;

        DataOutput out = serialize.getDataOutput(128);
        out.writeObjectNotNull(t1);
        out.writeClassAndObject(t2);
        out.writeObject(t3,TEnum.class);
        out.writeClassAndObject(t4);

        ReadBuffer b = out.getStaticBuffer().asReadBuffer();
        assertEquals(t1, serialize.readObjectNotNull(b, TClass1.class));
        assertEquals(t2, serialize.readClassAndObject(b));
        assertEquals(t3, serialize.readObject(b,TEnum.class));
        assertEquals(t4, serialize.readClassAndObject(b));

        assertFalse(b.hasRemaining());
    }

    protected void multipleStringWrite() {
        String base = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"; //26 chars
        int no = 100;
        DataOutput out = serialize.getDataOutput(128);
        for (int i = 0; i < no; i++) {
            String str = base + (i + 1);
            out.writeObjectNotNull(str);
        }
        ReadBuffer b = out.getStaticBuffer().asReadBuffer();
        if (printStats) log.debug(bufferStats(b));
        for (int i = 0; i < no; i++) {
            String str = base + (i + 1);
            String read = serialize.readObjectNotNull(b, String.class);
            assertEquals(str, read);
        }
        assertFalse(b.hasRemaining());
    }


    protected String bufferStats(ReadBuffer b) {
        return "ReadBuffer length: " + b.length();
    }

}
