package com.thinkaurelius.titan.core.attribute;

import com.thinkaurelius.titan.graphdb.database.serialize.attribute.AbstractDecimal;

/**
 * Data type for a decimal value that can be used in the definition of a {@link com.thinkaurelius.titan.core.PropertyKey}
 * instead of using {@link Double} since precision values have a fixed number of decimals places, namely 6, and as such allow
 * for order preserving serialization.
 * In other words, use this data type when only up to 6 decimal places are needed and ordering is important in the definition
 * of vertex centric indices.
 *
 * @see Decimal
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class Precision extends AbstractDecimal {

    public static final int DECIMALS = 6;

    public static final Precision MIN_VALUE = new Precision(minDoubleValue(DECIMALS));
    public static final Precision MAX_VALUE = new Precision(maxDoubleValue(DECIMALS));

    private Precision() {}

    public Precision(double value) {
        super(value, DECIMALS);
    }

    private Precision(long format) {
        super(format, DECIMALS);
    }

    public static class PrecisionSerializer extends AbstractDecimalSerializer<Precision> {

        public PrecisionSerializer() {
            super(DECIMALS, Precision.class);
        }

        @Override
        protected Precision construct(long format, int decimals) {
            assert decimals==DECIMALS;
            return new Precision(format);
        }

    }

}
