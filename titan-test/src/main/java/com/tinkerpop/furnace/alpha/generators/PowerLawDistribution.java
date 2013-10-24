package com.tinkerpop.furnace.alpha.generators;

import java.util.Random;

/**
 * Generates values according to a scale-free distribution with the configured gamma value.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class PowerLawDistribution implements Distribution {

    private final double gamma;
    private final double multiplier;

    /**
     * Constructs a new scale-free distribution for the provided gamma value.
     *
     * @param gamma
     */
    public PowerLawDistribution(double gamma) {
        this(gamma,0.0);
    }

    private PowerLawDistribution(double gamma, double multiplier) {
        if (gamma <=2.0) throw new IllegalArgumentException("Beta must be bigger than 2: " + gamma);
        if (multiplier<0) throw new IllegalArgumentException("Invalid multiplier value: " + multiplier);
        this.gamma = gamma;
        this.multiplier=multiplier;
    }

    @Override
    public Distribution initialize(int invocations, int expectedTotal) {
        double multiplier = expectedTotal /((gamma -1)/(gamma -2) * invocations) * 2; //times two because we are generating stubs
        assert multiplier>0;
        return new PowerLawDistribution(gamma,multiplier);
    }

    @Override
    public int nextValue(Random random) {
        if (multiplier==0.0) throw new IllegalStateException("Distribution has not been initialized");
        return getValue(random,multiplier, gamma);
    }

    @Override
    public int nextConditionalValue(Random random, int otherValue) {
        return nextValue(random);
    }
    
    public static int getValue(Random random, double multiplier, double beta) {
        return (int)Math.round(multiplier*(Math.pow(1.0/(1.0-random.nextDouble()), 1.0/(beta-1.0))-1.0));
    }
}
