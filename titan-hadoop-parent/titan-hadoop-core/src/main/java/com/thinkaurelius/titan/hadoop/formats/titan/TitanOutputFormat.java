package com.thinkaurelius.titan.hadoop.formats.titan;

import com.thinkaurelius.titan.diskstorage.configuration.ConfigOption;
import com.thinkaurelius.titan.hadoop.FaunusVertex;
import com.thinkaurelius.titan.hadoop.Holder;
import com.thinkaurelius.titan.hadoop.compat.HadoopCompiler;
import com.thinkaurelius.titan.hadoop.config.ModifiableHadoopConfiguration;
import com.thinkaurelius.titan.hadoop.config.TitanHadoopConfiguration;
import com.thinkaurelius.titan.hadoop.formats.MapReduceFormat;
import com.thinkaurelius.titan.hadoop.formats.noop.NoOpOutputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

import static com.thinkaurelius.titan.hadoop.compat.HadoopCompatLoader.DEFAULT_COMPAT;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class TitanOutputFormat extends NoOpOutputFormat implements MapReduceFormat {
    @Override
    public void addMapReduceJobs(final HadoopCompiler compiler) {

        final boolean inferSchema =
                ModifiableHadoopConfiguration.of(compiler.getConf()).get(TitanHadoopConfiguration.OUTPUT_INFER_SCHEMA);

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

        Configuration outputConf = compiler.getConf();

        outputConf.setBoolean(DEFAULT_COMPAT.getSpeculativeMapConfigKey(), false);
        outputConf.setBoolean(DEFAULT_COMPAT.getSpeculativeReduceConfigKey(), false);

        compiler.addMapReduce(TitanGraphOutputMapReduce.VertexMap.class,
                null,
                TitanGraphOutputMapReduce.Reduce.class,
                LongWritable.class,
                Holder.class,
                NullWritable.class,
                FaunusVertex.class,
                outputConf);
        compiler.addMap(TitanGraphOutputMapReduce.EdgeMap.class,
                NullWritable.class,
                FaunusVertex.class,
                outputConf);
    }
}
