package com.thinkaurelius.titan.graphdb.serializer;


import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class KryoTest {

    private static final Logger log = LoggerFactory.getLogger(KryoTest.class);

    TestClass a = new TestClass(50, 100, new short[]{1, 2, 3, 4}, TestEnum.One);
    String[] b = {"Hello", "John"};

    boolean printStats = true;

    @Before
    public void setUp() throws Exception {
    }

    @Test(expected = IllegalArgumentException.class)
    public void kryoUnregisteredErrorTest1() {
        Kryo serial = new Kryo();
        serial.setRegistrationRequired(true);
        serial.writeObject(new Output(100), a);
    }

    @Test(expected = IllegalArgumentException.class)
    public void kryoUnregisteredErrorTest2() {
        Kryo serial = new Kryo();
        serial.setRegistrationRequired(true);
        serial.writeObject(new Output(100), b);
    }

    @Test
    public void kryoUnregisteredTest() {
        Kryo serial = new Kryo();
        serial.setRegistrationRequired(false);
        Output b1 = new Output(100), b2 = new Output(100);
        serial.writeObject(b1, a);
        serial.writeObject(b2, b);

        Input i1 = new Input(b1.getBuffer(),0,b1.position()), i2 = new Input(b2.getBuffer(),0,b2.position());
        Kryo serial2 = new Kryo();
        serial2.setRegistrationRequired(false);
        assertTrue(Arrays.equals(b, serial2.readObject(i2, b.getClass())));
        assertEquals(a, serial2.readObject(i1, a.getClass()));

        Kryo serial3 = new Kryo();
        serial3.register(a.getClass());
        serial3.register(short[].class);
        serial3.register(TestEnum.class);
        Output b3 = new Output(100);
        serial3.writeObject(b3, a);
    }

    @Test
    public void testNumber() {
        Kryo kryo = new Kryo();
        Output out = new Output(100);
        String s = "Hello world";
        int five = 5;
        kryo.writeObject(out,s);
        kryo.writeObject(out,five);


    }

    enum E {
        E1 {}, //Assuming some method definitions go here - left blank for simplicity

        E2 {};
    }

    @Test
    public void testKryoEnum() {
        Kryo kryo = new Kryo();
        kryo.register(E.class);
        Output b = new Output(100);
        kryo.writeObject(b, E.E1);
        Input i = new Input(b.getBuffer(),0,b.position());
        E instance = kryo.readObject(i, E.class);
    }

    @After
    public void tearDown() throws Exception {
    }

}
