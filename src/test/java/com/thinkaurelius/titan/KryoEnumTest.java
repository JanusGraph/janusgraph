package com.thinkaurelius.titan;

import com.esotericsoftware.kryo.Kryo;

import java.nio.ByteBuffer;

public class KryoEnumTest {

	public static void main(String[] args)  {
		Kryo kryo = new Kryo();
		kryo.register(E.class);
		ByteBuffer b = ByteBuffer.allocate(100);
		kryo.writeObjectData(b, E.E1);
		b.rewind();
		E instance = kryo.readObjectData(b, E.class);
		
	}


	
}

enum E {
	E1 { }, //Assuming some method definitions go here - left blank for simplicity
	
	E2 { };
}

