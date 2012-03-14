package com.thinkaurelius.titan.diskstorage;

import java.nio.ByteBuffer;

/**
 * An entry is the primitive persistence unit used in the graph database middleware.
 *
 * An entry consists of a column and value both of which are general {@link java.nio.ByteBuffer}s.
 * 
 * @author	Matthias Br&ouml;cheler (me@matthiasb.com);
 *
 */
public class Entry {

	private final ByteBuffer column;
	private final ByteBuffer value;
	
	public Entry(ByteBuffer column, ByteBuffer value) {
		assert column!=null;
		this.column=column;
		this.value=value;
	}
	
	/**
	 * Returns the column ByteBuffer of this entry.
	 * 
	 * @return Column ByteBuffer
	 */
	public ByteBuffer getColumn() {
		return column;
	}


	/**
	 * Returns the value ByteBuffer of this entry.
	 * 
	 * @return Value ByteBuffer
	 */
	public ByteBuffer getValue() {
		return value;
	}


	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((column == null) ? 0 : column.hashCode());
		result = prime * result + ((value == null) ? 0 : value.hashCode());
		return result;
	}


	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Entry other = (Entry) obj;
		if (column == null) {
			if (other.column != null)
				return false;
		} else if (!column.equals(other.column))
			return false;
		if (value == null) {
			if (other.value != null)
				return false;
		} else if (!value.equals(other.value))
			return false;
		return true;
	}
}
