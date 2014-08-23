package com.thinkaurelius.titan.hadoop;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.thinkaurelius.titan.diskstorage.configuration.ConfigElement;
import com.thinkaurelius.titan.hadoop.config.TitanHadoopConfiguration;

import java.io.InputStreamReader;
import java.io.IOException;
import java.util.Properties;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class Tokens {

    private static final Logger log = LoggerFactory.getLogger(Tokens.class);

    public static final String TITAN_HADOOP_PIPELINE_MAP_SPILL_OVER = ConfigElement.getPath(TitanHadoopConfiguration.PIPELINE_MAP_SPILL_OVER);
    public static final String TITAN_HADOOP_PIPELINE_TRACK_PATHS = ConfigElement.getPath(TitanHadoopConfiguration.PIPELINE_TRACK_PATHS);
    public static final String TITAN_HADOOP_PIPELINE_TRACK_STATE = ConfigElement.getPath(TitanHadoopConfiguration.PIPELINE_TRACK_STATE);

    public enum Action {DROP, KEEP} // TODO check whether this really belongs here

    private static final String NAMESPACE = "titan.hadoop.mapreduce";

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
    public static final String _LINK = "_link";
    public static final String _VALUE = "_value";
    public static final String LABEL = "label";
    public static final String _LABEL = "_label";
    public static final String NULL = "null";
    public static final String TAB = "\t";
    public static final String NEWLINE = "\n";
    public static final String EMPTY_STRING = "";

    public static final String TITAN_HADOOP_HOME = "TITAN_HADOOP_HOME";

    public static final String PART = "part";
    public static final String GRAPH = "graph";
    public static final String SIDEEFFECT = "sideeffect";
    public static final String JOB = "job";

    public static final String BZ2 = "bz2";

    public static int DEFAULT_MAP_SPILL_OVER = 500;

}
