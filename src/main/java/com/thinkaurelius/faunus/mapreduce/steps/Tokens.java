package com.thinkaurelius.faunus.mapreduce.steps;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class Tokens {
    public enum Action {DROP, KEEP}

    private static final String NAMESPACE = "faunus.algebra";

    public static String makeNamespace(final Class klass) {
        return NAMESPACE + "." + klass.getSimpleName().toLowerCase();
    }

    public static final String SETUP = "setup";
    public static final String MAP = "map";
    public static final String REDUCE = "reduce";
}
