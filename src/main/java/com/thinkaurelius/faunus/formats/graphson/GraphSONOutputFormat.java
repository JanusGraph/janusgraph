package com.thinkaurelius.faunus.formats.graphson;

import com.thinkaurelius.faunus.FaunusVertex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Adopted from Hadoop's TextOutputFormat source code.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GraphSONOutputFormat extends FileOutputFormat<NullWritable, FaunusVertex> {

    public RecordWriter<NullWritable, FaunusVertex> getRecordWriter(final TaskAttemptContext job) throws IOException, InterruptedException {
        final Configuration conf = job.getConfiguration();
        boolean isCompressed = getCompressOutput(job);
        CompressionCodec codec = null;
        String extension = "";
        if (isCompressed) {
            final Class<? extends CompressionCodec> codecClass = getOutputCompressorClass(job, BZip2Codec.class);
            codec = ReflectionUtils.newInstance(codecClass, conf);
            extension = codec.getDefaultExtension();
        }
        final Path file = getDefaultWorkFile(job, extension);
        final FileSystem fs = file.getFileSystem(conf);
        if (!isCompressed) {
            FSDataOutputStream fileOut = fs.create(file, false);
            return new GraphSONRecordWriter(fileOut);
        } else {
            FSDataOutputStream fileOut = fs.create(file, false);
            return new GraphSONRecordWriter(new DataOutputStream(codec.createOutputStream(fileOut)));
        }
    }
}