package com.thinkaurelius.titan.graphdb.serializer;


import com.thinkaurelius.titan.diskstorage.ReadBuffer;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.graphdb.database.serialize.DataOutput;
import com.thinkaurelius.titan.graphdb.database.serialize.Serializer;
import com.thinkaurelius.titan.graphdb.database.serialize.attribute.DoubleSerializer;
import com.thinkaurelius.titan.graphdb.database.serialize.attribute.FloatSerializer;
import com.thinkaurelius.titan.graphdb.database.serialize.kryo.KryoSerializer;
import com.thinkaurelius.titan.testutil.PerformanceTest;
import com.thinkaurelius.titan.testutil.RandomGenerator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Calendar;

import static com.thinkaurelius.titan.graphdb.database.serialize.SerializerInitialization.RESERVED_ID_OFFSET;
import static org.junit.Assert.*;

public class SerializerTest {

    private static final Logger log =
            LoggerFactory.getLogger(SerializerTest.class);

    Serializer serialize;
    boolean printStats;

    @Before
    public void setUp() throws Exception {
        serialize = new KryoSerializer(false);
        serialize.registerClass(TestEnum.class, RESERVED_ID_OFFSET + 1);
        serialize.registerClass(TestClass.class, RESERVED_ID_OFFSET + 2);
        serialize.registerClass(short[].class, RESERVED_ID_OFFSET + 3);

        printStats = true;
    }

    @Test
    public void objectWriteRead() {
        //serialize.registerClass(short[].class);
        //serialize.registerClass(TestClass.class);
        DataOutput out = serialize.getDataOutput(128, true);
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

    @Test
    public void stringSerialization() {
        //Characters
        DataOutput out = serialize.getDataOutput(((int) Character.MAX_VALUE) * 2 + 8, true);
        for (char c = Character.MIN_VALUE; c < Character.MAX_VALUE; c++) {
            out.writeObjectNotNull(Character.valueOf(c));
        }
        ReadBuffer b = out.getStaticBuffer().asReadBuffer();
        for (char c = Character.MIN_VALUE; c < Character.MAX_VALUE; c++) {
            assertEquals(c, serialize.readObjectNotNull(b, Character.class).charValue());
        }


        //String
        for (int t = 0; t < 10000; t++) {
            DataOutput out1 = serialize.getDataOutput(32 + 5, true);
            DataOutput out2 = serialize.getDataOutput(32 + 5, true);
            String s1 = RandomGenerator.randomString(1, 32);
            String s2 = RandomGenerator.randomString(1, 32);
            out1.writeObjectNotNull(s1);
            out2.writeObjectNotNull(s2);
            StaticBuffer b1 = out1.getStaticBuffer();
            StaticBuffer b2 = out2.getStaticBuffer();
            assertEquals(s1, serialize.readObjectNotNull(b1.asReadBuffer(), String.class));
            assertEquals(s2, serialize.readObjectNotNull(b2.asReadBuffer(), String.class));
            assertEquals(s1 + " vs " + s2, Integer.signum(s1.compareTo(s2)), Integer.signum(b1.compareTo(b2)));
        }
    }

    @Test
    public void classSerialization() {
        DataOutput out = serialize.getDataOutput(128, true);
        out.writeObjectNotNull(Boolean.class);
        out.writeObjectNotNull(Byte.class);
        out.writeObjectNotNull(Double.class);
        ReadBuffer b = out.getStaticBuffer().asReadBuffer();
        assertEquals(Boolean.class, serialize.readObjectNotNull(b, Class.class));
        assertEquals(Byte.class, serialize.readObjectNotNull(b, Class.class));
        assertEquals(Double.class, serialize.readObjectNotNull(b, Class.class));
    }

    @Test
    public void parallelDeserialization() throws InterruptedException {
        DataOutput out = serialize.getDataOutput(128, true);
        out.putLong(8);
        out.writeClassAndObject(Long.valueOf(8));
        TestClass c = new TestClass(5, 8, new short[]{1, 2, 3, 4, 5}, TestEnum.Two);
        out.writeObject(c, TestClass.class);
        final StaticBuffer b = out.getStaticBuffer();

        int numThreads = 100;
        Thread[] threads = new Thread[numThreads];
        for (int i = 0; i < numThreads; i++) {
            threads[i] = new Thread(new Runnable() {
                @Override
                public void run() {
                    for (int j = 0; j < 100000; j++) {
                        ReadBuffer c = b.asReadBuffer();
                        assertEquals(8, c.getLong());
                        Long l = (Long) serialize.readClassAndObject(c);
                        assertEquals(8, l.longValue());
                        TestClass c2 = serialize.readObjectNotNull(c, TestClass.class);
                    }
                }
            });
            threads[i].start();
        }
        for (int i = 0; i < numThreads; i++) {
            threads[i].join();
        }
    }

    @Test
    public void primitiveSerialization() {
        DataOutput out = serialize.getDataOutput(128, true);
        out.writeObjectNotNull(Boolean.FALSE);
        out.writeObjectNotNull(Boolean.TRUE);
        out.writeObjectNotNull(Byte.MIN_VALUE);
        out.writeObjectNotNull(Byte.MAX_VALUE);
        out.writeObjectNotNull(new Byte((byte) 0));
        out.writeObjectNotNull(Short.MIN_VALUE);
        out.writeObjectNotNull(Short.MAX_VALUE);
        out.writeObjectNotNull(new Short((short) 0));
        out.writeObjectNotNull(Character.MIN_VALUE);
        out.writeObjectNotNull(Character.MAX_VALUE);
        out.writeObjectNotNull(new Character('a'));
        out.writeObjectNotNull(Integer.MIN_VALUE);
        out.writeObjectNotNull(Integer.MAX_VALUE);
        out.writeObjectNotNull(new Integer(0));
        out.writeObjectNotNull(Long.MIN_VALUE);
        out.writeObjectNotNull(Long.MAX_VALUE);
        out.writeObjectNotNull(new Long(0));
        out.writeObjectNotNull(FloatSerializer.MIN_VALUE);
        out.writeObjectNotNull(FloatSerializer.MAX_VALUE);
        out.writeObjectNotNull(new Float((float) 0.0));
        out.writeObjectNotNull(DoubleSerializer.MIN_VALUE);
        out.writeObjectNotNull(DoubleSerializer.MAX_VALUE);
        out.writeObjectNotNull(new Double(0.0));

        ReadBuffer b = out.getStaticBuffer().asReadBuffer();
        assertEquals(Boolean.FALSE, serialize.readObjectNotNull(b, Boolean.class));
        assertEquals(Boolean.TRUE, serialize.readObjectNotNull(b, Boolean.class));
        assertEquals(Byte.MIN_VALUE, serialize.readObjectNotNull(b, Byte.class).longValue());
        assertEquals(Byte.MAX_VALUE, serialize.readObjectNotNull(b, Byte.class).longValue());
        assertEquals(0, serialize.readObjectNotNull(b, Byte.class).longValue());
        assertEquals(Short.MIN_VALUE, serialize.readObjectNotNull(b, Short.class).longValue());
        assertEquals(Short.MAX_VALUE, serialize.readObjectNotNull(b, Short.class).longValue());
        assertEquals(0, serialize.readObjectNotNull(b, Short.class).longValue());
        assertEquals(Character.MIN_VALUE, serialize.readObjectNotNull(b, Character.class).charValue());
        assertEquals(Character.MAX_VALUE, serialize.readObjectNotNull(b, Character.class).charValue());
        assertEquals(new Character('a'), serialize.readObjectNotNull(b, Character.class));
        assertEquals(Integer.MIN_VALUE, serialize.readObjectNotNull(b, Integer.class).longValue());
        assertEquals(Integer.MAX_VALUE, serialize.readObjectNotNull(b, Integer.class).longValue());
        assertEquals(0, serialize.readObjectNotNull(b, Integer.class).longValue());
        assertEquals(Long.MIN_VALUE, serialize.readObjectNotNull(b, Long.class).longValue());
        assertEquals(Long.MAX_VALUE, serialize.readObjectNotNull(b, Long.class).longValue());
        assertEquals(0, serialize.readObjectNotNull(b, Long.class).longValue());
        assertEquals(FloatSerializer.MIN_VALUE, serialize.readObjectNotNull(b, Float.class).floatValue(), 1e-20);
        assertEquals(FloatSerializer.MAX_VALUE, serialize.readObjectNotNull(b, Float.class).floatValue(), 1e-20);
        assertEquals(0.0, serialize.readObjectNotNull(b, Float.class).floatValue(), 1e-20);
        assertEquals(DoubleSerializer.MIN_VALUE, serialize.readObjectNotNull(b, Double.class).doubleValue(), 1e-40);
        assertEquals(DoubleSerializer.MAX_VALUE, serialize.readObjectNotNull(b, Double.class).doubleValue(), 1e-40);
        assertEquals(0.0, serialize.readObjectNotNull(b, Double.class).doubleValue(), 1e-20);

    }


    @Test
    public void testObjectVerification() {
        KryoSerializer s = new KryoSerializer(true);
        DataOutput out = s.getDataOutput(128, true);
        Long l = Long.valueOf(128);
        out.writeClassAndObject(l);
        Calendar c = Calendar.getInstance();
        out.writeClassAndObject(c);
        NoDefaultConstructor dc = new NoDefaultConstructor(5);
        try {
            out.writeClassAndObject(dc);
            fail();
        } catch (IllegalArgumentException e) {

        }
        TestTransientClass d = new TestTransientClass(101);
        try {
            out.writeClassAndObject(d);
            fail();
        } catch (IllegalArgumentException e) {

        }
        out.writeObject(null, TestClass.class);
    }


    @Test
    public void longWriteTest() {
        String base = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"; //26 chars
        int no = 100;
        DataOutput out = serialize.getDataOutput(128, true);
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

    @Test
    public void largeWriteTest() {
        String base = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"; //26 chars
        String str = "";
        for (int i = 0; i < 100; i++) str += base;
        DataOutput out = serialize.getDataOutput(128, true);
        out.writeObjectNotNull(str);
        ReadBuffer b = out.getStaticBuffer().asReadBuffer();
        if (printStats) log.debug(bufferStats(b));
        assertEquals(str, serialize.readObjectNotNull(b, String.class));
        assertFalse(b.hasRemaining());
    }

    @Test
    public void enumSerializeTest() {
        DataOutput out = serialize.getDataOutput(128, true);
        out.writeObjectNotNull(TestEnum.Two);
        ReadBuffer b = out.getStaticBuffer().asReadBuffer();
        if (printStats) log.debug(bufferStats(b));
        assertEquals(TestEnum.Two, serialize.readObjectNotNull(b, TestEnum.class));
        assertFalse(b.hasRemaining());

    }

    @Test
    public void performanceTestLong() {
        int runs = 10000;
        printStats = false;
        PerformanceTest p = new PerformanceTest(true);
        for (int i = 0; i < runs; i++) {
            longWriteTest();
        }
        p.end();
        log.debug("LONG: Avg micro time: " + (p.getMicroTime() / runs));
    }

    @Test
    public void performanceTestShort() {
        int runs = 10000;
        printStats = false;
        PerformanceTest p = new PerformanceTest(true);
        for (int i = 0; i < runs; i++) {
            objectWriteRead();
        }
        p.end();
        log.debug("SHORT: Avg micro time: " + (p.getMicroTime() / runs));
    }

    public static String bufferStats(ReadBuffer b) {
        return "ReadBuffer length: " + b.length();
    }

    @Test(expected = IllegalArgumentException.class)
    public void checkNonObject() {
        DataOutput out = serialize.getDataOutput(128, false);
        out.writeObject("This is a test", String.class);
    }


    @After
    public void tearDown() throws Exception {
    }

}

