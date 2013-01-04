package com.thinkaurelius.faunus.formats.titan;

import com.thinkaurelius.faunus.FaunusVertex;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class TitanInputFormat extends InputFormat<NullWritable, FaunusVertex> implements Configurable {

    public static final String TITAN_GRAPH_INPUT_STORAGE_BACKEND = "titan.graph.input.storage.backend";
    public static final String TITAN_GRAPH_INPUT_STORAGE_HOSTNAME = "titan.graph.input.storage.hostname";
    public static final String TITAN_GRAPH_INPUT_STORAGE_PORT = "titan.graph.input.storage.port";
}
