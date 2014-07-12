package com.thinkaurelius.titan.hadoop.formats.titan;

import com.thinkaurelius.titan.diskstorage.configuration.ConfigOption;
import com.thinkaurelius.titan.hadoop.FaunusVertex;
import com.thinkaurelius.titan.hadoop.Holder;
import com.thinkaurelius.titan.hadoop.compat.HadoopCompiler;
import com.thinkaurelius.titan.hadoop.config.ConfigurationUtil;
import com.thinkaurelius.titan.hadoop.config.TitanHadoopConfiguration;
import com.thinkaurelius.titan.hadoop.formats.MapReduceFormat;
import com.thinkaurelius.titan.hadoop.formats.noop.NoOpOutputFormat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class TitanOutputFormat extends NoOpOutputFormat implements MapReduceFormat {

//    public static final String TITAN_HADOOP_GRAPH_OUTPUT_TITAN = "titan.hadoop.output";
//    public static final String TITAN_HADOOP_GRAPH_OUTPUT_TITAN_INFER_SCHEMA = "titan.hadoop.output.infer-schema";

    public static final ConfigOption<Boolean> INFER_SCHEMA = new ConfigOption<Boolean>(
            TitanHadoopConfiguration.OUTPUT_NS, "infer-schema",
            "Whether to attempt to automatically create Titan property keys and labels before writing data",
            ConfigOption.Type.LOCAL, true);

    @Override
    public void addMapReduceJobs(final HadoopCompiler compiler) {

        final boolean inferSchema = ConfigurationUtil.get(compiler.getConf(), INFER_SCHEMA);

        if (inferSchema) {
            compiler.addMapReduce(SchemaInferencerMapReduce.Map.class,
                    null,
                    SchemaInferencerMapReduce.Reduce.class,
                    LongWritable.class,
                    FaunusVertex.class,
                    NullWritable.class,
                    FaunusVertex.class,
                    SchemaInferencerMapReduce.createConfiguration());
        }
        compiler.addMapReduce(TitanGraphOutputMapReduce.VertexMap.class,
                null,
                TitanGraphOutputMapReduce.Reduce.class,
                LongWritable.class,
                Holder.class,
                NullWritable.class,
                FaunusVertex.class,
                TitanGraphOutputMapReduce.createConfiguration());
        compiler.addMap(TitanGraphOutputMapReduce.EdgeMap.class,
                NullWritable.class,
                FaunusVertex.class,
                TitanGraphOutputMapReduce.createConfiguration());
    }
}
