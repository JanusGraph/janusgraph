package com.thinkaurelius.titan.graphdb.serializer.test;


import com.esotericsoftware.kryo.Kryo;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Arrays;

import static com.thinkaurelius.titan.graphdb.serializer.test.SerializerUnitTest.bufferStats;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class KryoTest {

	private static final Logger log = LoggerFactory.getLogger(KryoTest.class);
	
	TestClass a = new TestClass(50,100,new short[]{1,2,3,4}, TestEnum.One);
	String[] b = {"Hello", "John"}; 
	
	boolean printStats=true;
	
	@Before
	public void setUp() throws Exception {
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void kryoUnregisteredErrorTest1() {
		Kryo serial=new Kryo();
		serial.writeObjectData(ByteBuffer.allocate(100), a);
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void kryoUnregisteredErrorTest2() {
		Kryo serial=new Kryo();
		serial.writeObjectData(ByteBuffer.allocate(100), b);
	}
	
	@Test
	public void kryoUnregisteredTest() {
		Kryo serial=new Kryo();
		serial.setRegistrationOptional(true);
		ByteBuffer b1 = ByteBuffer.allocate(100), b2 = ByteBuffer.allocate(100);
		serial.writeObjectData(b1, a);
		b1.flip();
		serial.writeObject(b2,b);
		b2.flip();
		if (printStats) {
			log.debug(bufferStats(b1));
			log.debug(bufferStats(b2));
		}
		
		Kryo serial2 = new Kryo();
		serial2.setRegistrationOptional(true);
		assertTrue(Arrays.equals(b, serial2.readObject(b2, b.getClass())));
		assertEquals(a, serial2.readObjectData(b1, a.getClass()));
		
		Kryo serial3=new Kryo();
		serial3.register(a.getClass());
		serial3.register(short[].class);
		serial3.register(TestEnum.class);
		ByteBuffer b3 = ByteBuffer.allocate(100);
		serial3.writeObjectData(b3, a);
		b3.flip();
		if (printStats) log.debug(bufferStats(b3));
	}

	@After
	public void tearDown() throws Exception {
	}

}
