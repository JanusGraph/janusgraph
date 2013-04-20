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
    public static final String CLEANUP = "cleanup";
    public static final String _ID = "_id";
    public static final String ID = "id";
    public static final String _PROPERTIES = "_properties";
    public static final String _COUNT = "_count";
    public static final String LABEL = "label";
    public static final String NULL = "null";
    public static final String TAB = "\t";
    public static final String NEWLINE = "\n";
    public static final String EMPTY_STRING = "";

    public static final String VERSION = "0.3.0";
    public static final String FAUNUS_JOB_JAR = "faunus-" + VERSION + "-job.jar";
    public static final String FAUNUS_HOME = "FAUNUS_HOME";

    public static final String PART = "part";
    public static final String GRAPH = "graph";
    public static final String SIDEEFFECT = "sideeffect";
    public static final String JOB = "job";

    public static final String BZ2 = "bz2";

    public static int DEFAULT_MAP_SPILL_OVER = 500;
    public static final String FAUNUS_PIPELINE_MAP_SPILL_OVER = "faunus.pipeline.map-spill-over";

}
