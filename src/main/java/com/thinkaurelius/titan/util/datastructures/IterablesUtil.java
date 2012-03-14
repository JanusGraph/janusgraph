package com.thinkaurelius.titan.util.datastructures;

import com.google.common.collect.Iterators;

import java.util.Iterator;

public class IterablesUtil {

	public static final<O> Iterable<O> emptyIterable() {
		return new Iterable<O>(){

			@Override
			public Iterator<O> iterator() {
				return Iterators.emptyIterator();
			}
			
		};
	}
	
	
}
