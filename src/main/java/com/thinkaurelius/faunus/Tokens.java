package com.thinkaurelius.faunus;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class Tokens {

    public enum Action {DROP, KEEP}

    public enum Order {REVERSE, STANDARD}

    private static final String NAMESPACE = "faunus.mapreduce";

    public static String makeNamespace(final Class klass) {
        return NAMESPACE + "." + klass.getSimpleName().toLowerCase();
    }

    public static final String SETUP = "setup";
    public static final String MAP = "map";
    public static final String REDUCE = "reduce";
    public static final String _ID = "_id";
    public static final String _PROPERTIES = "_properties";
    public static final String NULL = "null";
    public static final String TAB = "\t";
    public static final String NEWLINE = "\n";
    public static final String EMPTY_STRING = "";
}
