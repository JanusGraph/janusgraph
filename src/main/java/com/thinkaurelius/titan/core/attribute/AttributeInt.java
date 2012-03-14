package com.thinkaurelius.titan.core.attribute;

/**
 * Provides a {@link RangeAttribute} implementation for integers.
 * 
 * RangeAttributes are needed for range-index property types. This implementation ensures that integer values are
 * properly ordered in the index.
 * 
 * @author	Matthias Br&ouml;cheler (me@matthiasb.com);
 * 
 */
public class AttributeInt implements RangeAttribute<AttributeInt> {

	private final int value;
	
	/**
	 * Constructs a new RangeInt with the given value
	 * @param value
	 */
	public AttributeInt(int value) {
		this.value=value;
	}
	
	@Override
	public int compareTo(AttributeInt arg) {
		return Integer.valueOf(value).compareTo(arg.value);
	}

	/**
	 * Returns the value of this RangeInt
	 * @return value of this RangeInt
	 */
	public int getIntValue() {
		return value;
	}

	
	@Override
	public String toString() {
		return "{" + value + "}";
	}
	
	@Override
	public boolean equals(Object other) {
		if (this==other) return true;
		else if (!getClass().isInstance(other)) return false;
		return value==((AttributeInt)other).value;
	}
	
	@Override
	public int hashCode() {
		return value;
	}
}
