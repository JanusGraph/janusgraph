package com.thinkaurelius.faunus.formats.titan;

import com.thinkaurelius.faunus.formats.noop.NoOpOutputFormat;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class TitanOutputFormat extends NoOpOutputFormat {

    public static final String TITAN_GRAPH_OUTPUT = "titan.graph.output";
    public static final String TITAN_GRAPH_OUTPUT_INFER_SCHEMA = "titan.graph.output.infer-schema";
}
