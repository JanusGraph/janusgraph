package com.thinkaurelius.titan.core.attribute;

/**
 * Provides a {@link RangeAttribute} implementation for longs.
 * 
 * RangeAttributes are needed for range-index property types. This implementation ensures that long values are
 * properly ordered in the index.
 * 
 * @author	Matthias Br&ouml;cheler (me@matthiasb.com);
 * 
 */
public class AttributeLong implements RangeAttribute<AttributeLong> {

	private final long value;
	
	/**
	 * Constructs a new RangeInt with the given value
	 * @param value
	 */
	public AttributeLong(long value) {
		this.value=value;
	}
	
	@Override
	public int compareTo(AttributeLong arg) {
		return Long.valueOf(value).compareTo(arg.getLongValue());
	}

	/**
	 * Returns the value of this RangeInt
	 * @return value of this RangeInt
	 */
	public long getLongValue() {
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
		return value==((AttributeLong)other).value;
	}
	
	@Override
	public int hashCode() {
		return (int)(value ^ value>>>32);
	}
	
}
