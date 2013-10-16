package com.tinkerpop.furnace.alpha.generators;

import java.util.Random;

/**
 * CopyDistribution returns the conditional value.
 *
 * Hence, this class can be used as the in-degree distribution to ensure that
 * the in-degree of a vertex is equal to the out-degree.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class CopyDistribution implements Distribution {

    @Override
    public Distribution initialize(int invocations, int expectedTotal) {
        return this;
    }

    @Override
    public int nextValue(Random random) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int nextConditionalValue(Random random, int otherValue) {
        return otherValue;
    }
}
