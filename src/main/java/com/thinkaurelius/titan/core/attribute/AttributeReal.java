package com.thinkaurelius.titan.core.attribute;


/**
 * {@link RangeAttribute} for real or floating point attribute values with a fixed number of decimal digits.
 * 
 * This data type should be used instead of {@link Double} or {@link Float} for property types which are range-indexed.
 * In contrast to such high precision floating point values, this implementation only supports a up to a fixed number
 * of decimal digits, namely {@link #DECIMALS}. However, it ensures that the attribute values are properly ordered
 * so that they can be queried using {@link Interval}s.
 * 
 * @author	Matthias Br&ouml;cheler (me@matthiasb.com);
 * 
 */
public class AttributeReal implements RangeAttribute<AttributeReal> {

	/**
	 * Maximum number of decimal digits supported. Value = {@value}
	 */
	public static final int DECIMALS = 6;
	private static int multiplier = 0;
	
	
	private final double value;
	
	/**
	 * Constructs a new real with given value.
	 * If the value has more than the fixed number of decimal digits, it will be rounded to fit.
	 * 
	 * @param value Value for this real
	 * @throws IllegalArgumentException if the value is too large to fit into a real.
	 */
	public AttributeReal(double value) {
		double val = value * getMultiplier();
		if(Math.abs(val)>Long.MAX_VALUE) throw new IllegalArgumentException("Number overflow: " + value);
		this.value = Math.rint(val)/getMultiplier();
	}	
	
	/**
	 * Returns the value of this real
	 * 
	 * @return value of this real
	 */
	public double getValue() {
		return value;
	}

	@Override
	public int compareTo(AttributeReal arg) {
		return Double.compare(value, arg.value);
	}
	
	public static int getMultiplier() {
		if (multiplier==0) {
			multiplier=1;
			for (int i=0;i<DECIMALS;i++) multiplier*=10;
		}
		return multiplier;
	}
	
	@Override
	public String toString() {
		return "{" + value + "}";
	}
	
	@Override
	public boolean equals(Object other) {
		if (this==other) return true;
		else if (!getClass().isInstance(other)) return false;
		return value==((AttributeReal)other).value;
	}
	
	@Override
	public int hashCode() {
		return (new Double(value)).hashCode();
	}
	
}
