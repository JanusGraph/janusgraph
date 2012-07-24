package com.thinkaurelius.faunus;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class Tokens {
    public enum Action {DROP, KEEP}

    private static final String NAMESPACE = "faunus.mapreduce";

    public static String makeNamespace(final Class klass) {
        return NAMESPACE + "." + klass.getSimpleName().toLowerCase();
    }

    public static final String SETUP = "setup";
    public static final String MAP = "map";
    public static final String REDUCE = "reduce";
    public static final String _ID = "_id";
    public static final String _PROPERTIES = "_properties";

    public static final String GRAPH_INPUT_FORMAT_CLASS = "faunus.graph.input.format.class";
    public static final String GRAPH_INPUT_LOCATION = "faunus.graph.input.location";
    public static final String GRAPH_INPUT_EDGE_DIRECTION_FILTER = "faunus.graph.input.edge.direction.filter";
    public static final String GRAPH_INPUT_EDGE_LABEL_FILTER_LABELS = "faunus.graph.input.edge.label.filter.labels";
    public static final String GRAPH_INPUT_EDGE_LABEL_FILTER_ACTION = "faunus.graph.input.edge.label.filter.action";
    public static final String GRAPH_INPUT_EDGE_PROPERTY_FILTER_KEYS = "faunus.graph.input.edge.property.filter.labels";
    public static final String GRAPH_INPUT_EDGE_PROPERTY_FILTER_ACTION = "faunus.graph.input.edge.property.filter.action";
    public static final String GRAPH_INPUT_VERTEX_PROPERTY_FILTER_KEYS = "faunus.graph.input.vertex.property.filter.labels";
    public static final String GRAPH_INPUT_VERTEX_PROPERTY_FILTER_ACTION = "faunus.graph.input.vertex.property.filter.action";

    public static final String GRAPH_OUTPUT_FORMAT_CLASS = "faunus.graph.output.format.class";
    public static final String STATISTIC_OUTPUT_FORMAT_CLASS = "faunus.statistic.output.format.class";
    public static final String DATA_OUTPUT_LOCATION = "faunus.data.output.location";
    public static final String OVERWRITE_DATA_OUTPUT_LOCATION = "faunus.overwrite.data.output.location";
}
