package com.thinkaurelius.titan.diskstorage;

import com.thinkaurelius.titan.diskstorage.util.ByteBufferUtil;
import com.thinkaurelius.titan.graphdb.database.serialize.DataOutput;
import com.thinkaurelius.titan.graphdb.database.serialize.Serializer;
import com.thinkaurelius.titan.graphdb.database.serialize.kryo.KryoSerializer;
import com.thinkaurelius.titan.testutil.RandomGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Arrays;

import static org.junit.Assert.assertTrue;

public class KeyValueStoreUtil {

	private static final Logger log = LoggerFactory.getLogger(KeyValueStoreUtil.class);
	public static final Serializer serial = new KryoSerializer(true);
	public static final long idOffset = 1000;
	
	public static String[] generateData(int numKeys) {
		String[] ret = new String[numKeys];
		for (int i=0;i<numKeys;i++) {
			ret[i]=RandomGenerator.randomString();
		}
		return ret;
	}
	
	public static String[][] generateData(int numKeys, int numColumns) {
		String[][] ret = new String[numKeys][numColumns];
		for (int i=0;i<numKeys;i++) {
			for (int j=0;j<numColumns;j++) {
				ret[i][j]= RandomGenerator.randomString();
			}
		}
		return ret;
	}
	
	public static void print(String[] data) {
		log.debug(Arrays.toString(data));
	}
	
	public static void print(String[][] data) {
		for (int i=0;i<data.length;i++) print(data[i]);
	}
	
	public static ByteBuffer getBuffer(int no) {
		return ByteBufferUtil.getLongByteBuffer(no+idOffset);
	}
	
	public static int getID(ByteBuffer b) {
		long res = b.getLong()-idOffset;
		assertTrue(res>=0 && res<Integer.MAX_VALUE);
		return (int)res;
	}
	
	public static ByteBuffer getBuffer(String s) {
		DataOutput out = serial.getDataOutput(50, true);
		out.writeObjectNotNull(s);
		return out.getByteBuffer();
	}
	
	public static String getString(ByteBuffer b) {
		return serial.readObjectNotNull(b, String.class);
	}
	
}
