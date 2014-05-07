package com.thinkaurelius.titan.core.attribute;

import com.thinkaurelius.titan.graphdb.database.serialize.attribute.AbstractDecimal;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class Precision extends AbstractDecimal {

    public static final int DECIMALS = 6;

    public static final Precision MIN_VALUE = new Precision(minDoubleValue(DECIMALS));
    public static final Precision MAX_VALUE = new Precision(maxDoubleValue(DECIMALS));


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
