package com.thinkaurelius.titan.hadoop.formats.titan;

import com.thinkaurelius.titan.hadoop.HadoopVertex;
import com.thinkaurelius.titan.hadoop.Holder;
import com.thinkaurelius.titan.hadoop.compat.HadoopCompiler;
import com.thinkaurelius.titan.hadoop.formats.MapReduceFormat;
import com.thinkaurelius.titan.hadoop.formats.noop.NoOpOutputFormat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class TitanOutputFormat extends NoOpOutputFormat implements MapReduceFormat {

    public static final String TITAN_HADOOP_GRAPH_OUTPUT_TITAN = "titan.hadoop.output";
    public static final String TITAN_HADOOP_GRAPH_OUTPUT_TITAN_INFER_SCHEMA = "titan.hadoop.output.infer-schema";

    @Override
    public void addMapReduceJobs(final HadoopCompiler compiler) {
        if (compiler.getConf().getBoolean(TITAN_HADOOP_GRAPH_OUTPUT_TITAN_INFER_SCHEMA, true)) {
            compiler.addMapReduce(SchemaInferencerMapReduce.Map.class,
                    null,
                    SchemaInferencerMapReduce.Reduce.class,
                    LongWritable.class,
                    HadoopVertex.class,
                    NullWritable.class,
                    HadoopVertex.class,
                    SchemaInferencerMapReduce.createConfiguration());
        }
        compiler.addMapReduce(TitanGraphOutputMapReduce.VertexMap.class,
                null,
                TitanGraphOutputMapReduce.Reduce.class,
                LongWritable.class,
                Holder.class,
                NullWritable.class,
                HadoopVertex.class,
                TitanGraphOutputMapReduce.createConfiguration());
        compiler.addMap(TitanGraphOutputMapReduce.EdgeMap.class,
                NullWritable.class,
                HadoopVertex.class,
                TitanGraphOutputMapReduce.createConfiguration());
    }
}
