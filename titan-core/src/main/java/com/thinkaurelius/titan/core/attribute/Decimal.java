package com.thinkaurelius.titan.core.attribute;

import com.thinkaurelius.titan.graphdb.database.serialize.attribute.AbstractDecimal;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class Decimal extends AbstractDecimal {

    public static final int DECIMALS = 3;

    public static final Decimal MIN_VALUE = new Decimal(minDoubleValue(DECIMALS));
    public static final Decimal MAX_VALUE = new Decimal(maxDoubleValue(DECIMALS));

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
