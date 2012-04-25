package com.thinkaurelius.titan;

import com.sleepycat.je.util.DbCacheSize;

import java.io.IOException;

public class TestBed {

	/**
	 * @param args
	 * @throws java.io.IOException
	 */
	public static void main(String[] args) throws IOException {
		byte b = (byte)(15 | (1<<7));
        System.out.println(b);
	}

	public static String toBinary(int b) {
		String res = Integer.toBinaryString(b);
		while (res.length()<32) res = "0" + res;
		return res;
		
	}
	

}
