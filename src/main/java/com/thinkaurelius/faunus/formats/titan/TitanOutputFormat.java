package com.thinkaurelius.faunus.formats.titan;

import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.Holder;
import com.thinkaurelius.faunus.formats.BlueprintsGraphOutputMapReduce;
import com.thinkaurelius.faunus.formats.MapReduceFormat;
import com.thinkaurelius.faunus.formats.noop.NoOpOutputFormat;
import com.thinkaurelius.faunus.mapreduce.FaunusCompiler;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class TitanOutputFormat extends NoOpOutputFormat implements MapReduceFormat {

    public static final String FAUNUS_GRAPH_OUTPUT_TITAN = "faunus.graph.output.titan";
    public static final String FAUNUS_GRAPH_OUTPUT_TITAN_INFER_SCHEMA = "faunus.graph.output.titan.infer-schema";

    public void addMapReduceJobs(final FaunusCompiler compiler) throws IOException {
        if (compiler.getConf().getBoolean(FAUNUS_GRAPH_OUTPUT_TITAN_INFER_SCHEMA, true)) {
            compiler.addMapReduce(SchemaInferencerMapReduce.Map.class, null,
                    SchemaInferencerMapReduce.Reduce.class,
                    LongWritable.class, FaunusVertex.class, NullWritable.class, FaunusVertex.class, null);
        }
        compiler.addMapReduce(BlueprintsGraphOutputMapReduce.Map.class, null,
                BlueprintsGraphOutputMapReduce.Reduce.class,
                LongWritable.class, Holder.class, NullWritable.class, FaunusVertex.class, null);

    }

}
