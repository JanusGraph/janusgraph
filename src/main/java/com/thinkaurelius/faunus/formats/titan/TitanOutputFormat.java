package com.thinkaurelius.faunus.formats.titan;

import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.Holder;
import com.thinkaurelius.faunus.formats.BlueprintsGraphOutputMapReduce;
import com.thinkaurelius.faunus.formats.JobConfigurationFormat;
import com.thinkaurelius.faunus.formats.MapReduceFormat;
import com.thinkaurelius.faunus.formats.noop.NoOpOutputFormat;
import com.thinkaurelius.faunus.hdfs.HDFSTools;
import com.thinkaurelius.faunus.mapreduce.FaunusCompiler;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class TitanOutputFormat extends NoOpOutputFormat implements MapReduceFormat, JobConfigurationFormat {

    public static final String FAUNUS_GRAPH_OUTPUT_TITAN = "faunus.graph.output.titan";
    public static final String FAUNUS_GRAPH_OUTPUT_TITAN_INFER_SCHEMA = "faunus.graph.output.titan.infer-schema";

    @Override
    public void addMapReduceJobs(final FaunusCompiler compiler) {
        if (compiler.getConf().getBoolean(FAUNUS_GRAPH_OUTPUT_TITAN_INFER_SCHEMA, true)) {
            compiler.addMapReduce(SchemaInferencerMapReduce.Map.class,
                    null,
                    SchemaInferencerMapReduce.Reduce.class,
                    LongWritable.class,
                    FaunusVertex.class,
                    NullWritable.class,
                    FaunusVertex.class,
                    SchemaInferencerMapReduce.createConfiguration());
        }
        compiler.addMapReduce(BlueprintsGraphOutputMapReduce.Map.class,
                null,
                BlueprintsGraphOutputMapReduce.Reduce.class,
                LongWritable.class,
                Holder.class,
                NullWritable.class,
                FaunusVertex.class,
                BlueprintsGraphOutputMapReduce.createConfiguration());
    }

    @Override
    public void updateJob(final Job job) throws InterruptedException, IOException {
        try {
            final Configuration configuration = job.getConfiguration();
            if (FileInputFormat.class.isAssignableFrom(job.getInputFormatClass())) {
                final Long splitSize = configuration.getLong("mapred.max.split.size", -1);
                if (splitSize == -1)
                    throw new InterruptedException("Can not determine the number of reduce tasks if mapred.max.split.size is not set");
                final Path[] paths = FileInputFormat.getInputPaths(job);
                final PathFilter filter = FileInputFormat.getInputPathFilter(job);
                final FileSystem fs = FileSystem.get(configuration);
                Long totalSize = 0l;
                for (final Path path : paths) {
                    totalSize = totalSize + HDFSTools.getFileSize(fs, path, filter);
                }
                final int reduceTasks = (int) (totalSize.doubleValue() / splitSize.doubleValue());
                job.setNumReduceTasks((reduceTasks == 0) ? 1 : reduceTasks);
            } else {
                if (-1 == configuration.getInt("mapred.reduce.tasks", -1)) {
                    throw new InterruptedException("The input to Titan is not in HDFS and source size can not be determined -- set mapred.reduce.tasks");
                }
            }
        } catch (final ClassNotFoundException e) {
            throw new InterruptedException(e.getMessage());
        }
    }
}
