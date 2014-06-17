package com.thinkaurelius.titan.core.attribute;

import com.thinkaurelius.titan.graphdb.database.serialize.attribute.AbstractDecimal;

/**
 * Data type for a decimal value that can be used in the definition of a {@link com.thinkaurelius.titan.core.PropertyKey}
 * instead of using {@link Double} since decimals have a fixed number of decimals places, namely 3, and as such allow
 * for order preserving serialization.
 * In other words, use this data type when only up to 3 decimal places are needed and ordering is important in the definition
 * of vertex centric indices.
 *
 * @see Precision
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class Decimal extends AbstractDecimal {

    public static final int DECIMALS = 3;

    public static final Decimal MIN_VALUE = new Decimal(minDoubleValue(DECIMALS));
    public static final Decimal MAX_VALUE = new Decimal(maxDoubleValue(DECIMALS));

    private Decimal() {}

    public Decimal(double value) {
        super(value, DECIMALS);
    }

    private Decimal(long format) {
        super(format, DECIMALS);
    }


    public static class DecimalSerializer extends AbstractDecimalSerializer<Decimal> {

        public DecimalSerializer() {
            super(DECIMALS, Decimal.class);
        }

        @Override
        protected Decimal construct(long format, int decimals) {
            assert decimals==DECIMALS;
            return new Decimal(format);
        }

    }

}
