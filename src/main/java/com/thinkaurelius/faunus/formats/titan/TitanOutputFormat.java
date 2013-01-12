package com.thinkaurelius.faunus.formats.titan;

import com.thinkaurelius.faunus.formats.noop.NoOpOutputFormat;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class TitanOutputFormat extends NoOpOutputFormat {

    public static final String FAUNUS_GRAPH_OUTPUT_TITAN = "faunus.graph.output.titan";
    public static final String FAUNUS_GRAPH_OUTPUT_TITAN_INFER_SCHEMA = "faunus.graph.output.titan.infer-schema";
}
