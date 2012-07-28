package com.thinkaurelius.faunus.formats.titan;

import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.hadoop.ColumnFamilyInputFormat;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.SortedMap;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class PlayDriver extends Configured implements Tool {

    static final String KEYSPACE = "faunus";
    static final String COLUMN_FAMILY = "User";


    public int run(String[] args) throws Exception {
        final FileSystem hdfs = FileSystem.get(this.getConf());
        Job job = new Job();
        job.setInputFormatClass(ColumnFamilyInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        Path output = new Path("cassandra.txt");
        if (hdfs.exists(output))
            hdfs.delete(output);
        FileOutputFormat.setOutputPath(job, output);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setJarByClass(PlayDriver.class);
        job.setMapperClass(PlayDriver.Map.class);

        job.getConfiguration().set("cassandra.thrift.port", "9160");
        job.getConfiguration().set("cassandra.thrift.address", "localhost");
        //ConfigHelper.setInputPartitioner(job.getConfiguration(), "org.apache.cassandra.dht.RandomPartitioner");
        ConfigHelper.setInputColumnFamily(job.getConfiguration(), KEYSPACE, COLUMN_FAMILY);
        SlicePredicate predicate = new SlicePredicate().setColumn_names(Arrays.asList(ByteBufferUtil.bytes("age"), ByteBufferUtil.bytes("first")));
        ConfigHelper.setInputSlicePredicate(job.getConfiguration(), predicate);

        job.waitForCompletion(true);
        return 0;

    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new PlayDriver(), args);
    }

    public static class Map extends Mapper<ByteBuffer, SortedMap<ByteBuffer, IColumn>, Text, Text> {

        @Override
        public void map(final ByteBuffer key, final SortedMap<ByteBuffer, IColumn> value, final Mapper<ByteBuffer, SortedMap<ByteBuffer, IColumn>, Text, Text>.Context context) throws IOException, InterruptedException {
            final StringBuffer string = new StringBuffer();
            for (java.util.Map.Entry<ByteBuffer, IColumn> entry : value.entrySet()) {
                string.append(ByteBufferUtil.string(entry.getKey())).append("\t").append(ByteBufferUtil.string(entry.getValue().value()));
            }

            context.write(new Text(ByteBufferUtil.string(key, Charset.forName("UTF-8"))), new Text(string.toString()));
        }
    }
}
