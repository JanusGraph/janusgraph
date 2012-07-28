package com.thinkaurelius.faunus;

import com.thinkaurelius.faunus.formats.json.JSONOutputFormat;
import com.thinkaurelius.faunus.formats.titan.TitanCassandraInputFormat;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Arrays;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class PlayDriver extends Configured implements Tool {

    static final String KEYSPACE = "faunus";
    static final String COLUMN_FAMILY = "User";


    public int run(String[] args) throws Exception {
        final FileSystem hdfs = FileSystem.get(this.getConf());
        Job job = new Job(this.getConf());
        job.setInputFormatClass(TitanCassandraInputFormat.class);
        job.setOutputFormatClass(JSONOutputFormat.class);
        Path output = new Path("cassandra.txt");
        if (hdfs.exists(output))
            hdfs.delete(output);
        FileOutputFormat.setOutputPath(job, output);

        job.setJarByClass(PlayDriver.class);
        job.setMapperClass(Mapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(FaunusVertex.class);

        job.getConfiguration().set("cassandra.thrift.port", "9160");
        job.getConfiguration().set("cassandra.thrift.address", "localhost");
        job.getConfiguration().set("cassandra.partitioner.class", "org.apache.cassandra.dht.RandomPartitioner");
        ConfigHelper.setInputColumnFamily(job.getConfiguration(), KEYSPACE, COLUMN_FAMILY);
        SlicePredicate predicate = new SlicePredicate().setColumn_names(Arrays.asList(ByteBufferUtil.bytes("age"), ByteBufferUtil.bytes("first")));
        ConfigHelper.setInputSlicePredicate(job.getConfiguration(), predicate);

        job.waitForCompletion(true);
        return 0;

    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new PlayDriver(), args);
    }

    public static class Map extends Mapper<NullWritable, FaunusVertex, NullWritable, FaunusVertex> {

        @Override
        public void map(final NullWritable key, final FaunusVertex value, final Mapper<NullWritable, FaunusVertex, NullWritable, FaunusVertex>.Context context) throws IOException, InterruptedException {
            context.write(key, value);
        }
    }
}
