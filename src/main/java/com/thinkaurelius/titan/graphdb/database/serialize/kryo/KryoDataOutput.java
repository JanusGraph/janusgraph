package com.thinkaurelius.titan.graphdb.database.serialize.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.ObjectBuffer;
import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.graphdb.database.serialize.DataOutput;

import java.nio.ByteBuffer;

public class KryoDataOutput implements DataOutput {

	private static final int initialKryoCapacity = 128;
	private static final int maxKryoCapacity = 10 * 1024 * 1024;
	
	private static final int capacityFactor = 2;
	
	private ByteBuffer buffer;
	
	private final ObjectBuffer objects;
	
	KryoDataOutput(int capacity) {
		this(capacity,null);
		
	}
	
	KryoDataOutput(int capacity, Kryo kryo) {
		buffer = ByteBuffer.allocate(capacity);
		if (kryo!=null) objects = new ObjectBuffer(kryo,initialKryoCapacity,maxKryoCapacity);
		else objects = null;
		
	}
	
	public DataOutput putLong(long val) {
		ensureCapacity(8);
		buffer.putLong(val);
		return this;
	}
	
	public DataOutput putInt(int val) {
		ensureCapacity(4);
		buffer.putInt(val);
		return this;
	}
	
	public DataOutput putShort(short val) {
		ensureCapacity(2);
		buffer.putShort(val);
		return this;
	}
	
	public DataOutput putByte(byte val) {
		ensureCapacity(1);
		buffer.put(val);
		return this;
	}
	
	private void ensureCapacity(int noBytes) {
		if (buffer.remaining()<noBytes) {
			ByteBuffer larger = ByteBuffer.allocate(Math.max(buffer.capacity() * capacityFactor,
															buffer.position()+noBytes+10));
			buffer.flip();
			larger.put(buffer);
			buffer = larger;
		}
	}
	
	private void append(byte[] bytes) {
		ensureCapacity(bytes.length);
		buffer.put(bytes);
	}
	
	public DataOutput writeObject(Object object) {
		Preconditions.checkArgument(objects!=null,"This DataOutput has not been initialized for object writing!");
		append(objects.writeObject(object));
		return this;
	}
	
	public DataOutput writeObjectNotNull(Object object) {
		Preconditions.checkArgument(objects!=null,"This DataOutput has not been initialized for object writing!");
		append(objects.writeObjectData(object));
		return this;
	}
	
	public DataOutput writeClassAndObject(Object object) {
		Preconditions.checkArgument(objects!=null,"This DataOutput has not been initialized for object writing!");
		append(objects.writeClassAndObject(object));		
		return this;
	}
	
	public ByteBuffer getByteBuffer() {
		buffer.flip();
		return buffer;
	}
	
}
