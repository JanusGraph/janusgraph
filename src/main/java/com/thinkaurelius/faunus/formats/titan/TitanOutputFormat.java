package com.thinkaurelius.faunus.formats.titan;

import com.thinkaurelius.faunus.formats.noop.NoOpOutputFormat;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class TitanOutputFormat extends NoOpOutputFormat {

    public static final String TITAN_GRAPH_OUTPUT_STORAGE_BACKEND = "titan.graph.output.storage.backend";
    public static final String TITAN_GRAPH_OUTPUT_STORAGE_HOSTNAME = "titan.graph.output.storage.hostname";
    public static final String TITAN_GRAPH_OUTPUT_STORAGE_PORT = "titan.graph.output.storage.port";

}
