package com.thinkaurelius.faunus.util;

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

    public static final String GRAPH_INPUT_FORMAT_CLASS = "graph.input.format.class";
    public static final String GRAPH_OUTPUT_FORMAT_CLASS = "graph.output.format.class";
    public static final String STATISTICS_OUTPUT_FORMAT_CLASS = "statistics.output.format.class";
    public static final String GRAPH_INPUT_LOCATION = "graph.input.location";
    public static final String DATA_OUTPUT_LOCATION = "data.output.location";
    public static final String OVERWRITE_DATA_OUTPUT_LOCATION = "overwrite.data.output.location";
}
