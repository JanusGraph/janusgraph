package com.thinkaurelius.faunus.mapreduce.steps;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class Tokens {
    
    private static final String NAMESPACE = "faunus.algebra";
    
    public static String makeNamespace(final Class klass) {
        return NAMESPACE + "." + klass.getSimpleName().toLowerCase();
    }
}
