package com.thinkaurelius.titan.graphdb.serializer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.thinkaurelius.titan.diskstorage.ReadBuffer;
import com.thinkaurelius.titan.graphdb.database.serialize.DataOutput;
import com.thinkaurelius.titan.graphdb.database.serialize.Serializer;
import com.thinkaurelius.titan.graphdb.database.serialize.StandardSerializer;

public class SerializerTestCommon {


    private static final Logger log =
            LoggerFactory.getLogger(SerializerTestCommon.class);

    protected Serializer serialize;
    protected boolean printStats;

    @Before
    public void setUp() throws Exception {
        serialize = new StandardSerializer();
        printStats = true;
    }

    protected void objectWriteRead() {
        //serialize.registerClass(short[].class);
        //serialize.registerClass(TestClass.class);
        DataOutput out = serialize.getDataOutput(128);
        String str = "This is a test";
        int i = 5;
        TestClass c = new TestClass(5, 8, new short[]{1, 2, 3, 4, 5}, TestEnum.Two);
        Number n = new Double(3.555);
        out.writeObjectNotNull(str);
        out.putInt(i);
        out.writeObject(c, TestClass.class);
        out.writeClassAndObject(n);
        ReadBuffer b = out.getStaticBuffer().asReadBuffer();
        if (printStats) log.debug(bufferStats(b));
        String str2 = serialize.readObjectNotNull(b, String.class);
        assertEquals(str, str2);
        if (printStats) log.debug(bufferStats(b));
        assertEquals(b.getInt(), i);
        TestClass c2 = serialize.readObject(b, TestClass.class);
        assertEquals(c, c2);
        if (printStats) log.debug(bufferStats(b));
        assertEquals(n, serialize.readClassAndObject(b));
        if (printStats) log.debug(bufferStats(b));
        assertFalse(b.hasRemaining());
    }

    protected void longWrite() {
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
