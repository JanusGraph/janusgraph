package com.thinkaurelius.faunus.formats.titan;

import com.thinkaurelius.faunus.FaunusVertex;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class TitanInputFormat extends InputFormat<NullWritable, FaunusVertex> implements Configurable {

    public static final String FAUNUS_GRAPH_INPUT_TITAN_STORAGE_HOSTNAME = "faunus.graph.input.titan.storage.hostname";
    public static final String FAUNUS_GRAPH_INPUT_TITAN_STORAGE_PORT = "faunus.graph.input.titan.storage.port";
    public static final String FAUNUS_GRAPH_INPUT_TITAN = "faunus.graph.input.titan";

    public static final String FAUNUS_GRAPH_INPUT_TITAN_COMPONENTS = "faunus.graph.input.titan.load-vertex-components";

    public static final String OUT_EDGES = "outEdges";
    public static final String IN_EDGES = "inEdges";
    public static final String PROPERTIES = "properties";

}
