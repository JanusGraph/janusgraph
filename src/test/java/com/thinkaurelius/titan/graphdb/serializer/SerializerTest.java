package com.thinkaurelius.titan.graphdb.serializer;


import com.thinkaurelius.titan.graphdb.types.*;
import com.thinkaurelius.titan.graphdb.types.TypeCategory;
import com.thinkaurelius.titan.graphdb.database.serialize.DataOutput;
import com.thinkaurelius.titan.graphdb.database.serialize.Serializer;
import com.thinkaurelius.titan.graphdb.database.serialize.kryo.KryoSerializer;
import com.thinkaurelius.titan.graphdb.types.group.StandardTypeGroup;
import com.thinkaurelius.titan.graphdb.types.system.SystemTypeManager;
import com.thinkaurelius.titan.testutil.PerformanceTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class SerializerTest {
	
	private static final Logger log =
		LoggerFactory.getLogger(SerializerTest.class);

	Serializer serialize;
	boolean printStats;
	
	@Before
	public void setUp() throws Exception {
		serialize = new KryoSerializer(false);
		serialize.registerClass(TestEnum.class);
		serialize.registerClass(TestClass.class);
		serialize.registerClass(short[].class);
		serialize.registerClass(StandardEdgeLabel.class);
		serialize.registerClass(Directionality.class);
		serialize.registerClass(TypeCategory.class);
		serialize.registerClass(TypeVisibility.class);
		serialize.registerClass(StandardTypeGroup.class);
		serialize.registerClass(StandardPropertyKey.class);
		
		printStats = true;
	}

	@Test
	public void objectWriteRead() {
		//serialize.registerClass(short[].class);
		//serialize.registerClass(TestClass.class);
		DataOutput out = serialize.getDataOutput(128, true);
		String str = "This is a test";
		int i = 5;
		TestClass c = new TestClass(5,8,new short[]{1,2,3,4,5},TestEnum.Two);
		Number n = new Double(3.555);
		out.writeObjectNotNull(str);
		out.putInt(i);
		out.writeObject(c);
		out.writeClassAndObject(n);
		ByteBuffer b = out.getByteBuffer();
		if (printStats) log.debug(bufferStats(b));
		String str2=serialize.readObjectNotNull(b, String.class);
		assertEquals(str,str2);
		assertEquals(b.getInt(),i);
		TestClass c2 = serialize.readObject(b, TestClass.class);
		assertEquals(c,c2);
		assertEquals(n,serialize.readClassAndObject(b));
		assertFalse(b.hasRemaining());
	}
	
	@Test
	public void longWriteTest() {
		String base = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"; //26 chars
		int no = 100;
		DataOutput out = serialize.getDataOutput(128, true);
		for (int i=0;i<no;i++) {
			String str = base + (i+1);
			out.writeObjectNotNull(str);
		}
		ByteBuffer b = out.getByteBuffer();
		if (printStats) log.debug(bufferStats(b));
		for (int i=0;i<no;i++) {
			String str = base + (i+1);
			String read = serialize.readObjectNotNull(b, String.class);
			assertEquals(str,read);
		}
		assertFalse(b.hasRemaining());
	}
	
	@Test
	public void largeWriteTest() {
		String base = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"; //26 chars
		String str = "";
		for (int i=0;i<100;i++) str+=base;
		DataOutput out = serialize.getDataOutput(128, true);
		out.writeObjectNotNull(str);
		ByteBuffer b = out.getByteBuffer();
		if (printStats) log.debug(bufferStats(b));
		assertEquals(str,serialize.readObjectNotNull(b, String.class));
		assertFalse(b.hasRemaining());
	}
	
	@Test
	public void enumSerializeTest() {
		DataOutput out = serialize.getDataOutput(128, true);
		out.writeObjectNotNull(TestEnum.Two);
		ByteBuffer b = out.getByteBuffer();
		if (printStats) log.debug(bufferStats(b));
		assertEquals(TestEnum.Two,serialize.readObjectNotNull(b, TestEnum.class));
		assertFalse(b.hasRemaining());

	}
	
	@Test
	public void serializeRelationshipType() {
		StandardEdgeLabel relType = new StandardEdgeLabel("testName", TypeCategory.Simple,
				Directionality.Directed, TypeVisibility.Modifiable,FunctionalType.NON_FUNCTIONAL,
				new String[]{},new String[]{}, SystemTypeManager.SYSTEM_TYPE_GROUP);
		StandardPropertyKey propType = new StandardPropertyKey("testName", TypeCategory.Simple,
				Directionality.Directed, TypeVisibility.Modifiable,FunctionalType.NON_FUNCTIONAL,
				new String[]{},new String[]{}, SystemTypeManager.SYSTEM_TYPE_GROUP,true,true,String.class);
		DataOutput out = serialize.getDataOutput(128, true);
		out.writeObjectNotNull(relType);
		out.writeObjectNotNull(propType);
		ByteBuffer b = out.getByteBuffer();
		if (printStats) log.debug(bufferStats(b));
		assertEquals("testName",serialize.readObjectNotNull(b, StandardEdgeLabel.class).name);
		assertEquals(String.class,serialize.readObjectNotNull(b, StandardPropertyKey.class).getDataType());
		assertFalse(b.hasRemaining());
	}
	
	@Test
	public void performanceTestLong() {
		int runs = 10000;
		printStats = false;
		PerformanceTest p = new PerformanceTest(true);
		for (int i=0;i<runs;i++) {
			longWriteTest();
		}
		p.end();
		log.debug("LONG: Avg micro time: " + (p.getMicroTime()/runs));		
	}
	
	@Test
	public void performanceTestShort() {
		int runs = 10000;
		printStats = false;
		PerformanceTest p = new PerformanceTest(true);
		for (int i=0;i<runs;i++) {
			objectWriteRead();
		}
		p.end();
		log.debug("SHORT: Avg micro time: " + (p.getMicroTime()/runs));		
	}
	
	public static String bufferStats(ByteBuffer b) {
		return "ByteBuffer size: " + b.limit() + " position: " + b.position();
	}
	
	@Test(expected=IllegalArgumentException.class) public void checkNonObject() {
		DataOutput out = serialize.getDataOutput(128, false);
		out.writeObject("This is a test");
	}
	
	
	
	@After
	public void tearDown() throws Exception {
	}

}

