package com.thinkaurelius.titan.hadoop.formats;

import com.thinkaurelius.titan.diskstorage.configuration.Configuration;
import com.thinkaurelius.titan.hadoop.FaunusVertex;

import com.thinkaurelius.titan.hadoop.config.ModifiableHadoopConfiguration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.DataOutputStream;
import java.io.IOException;

import static com.thinkaurelius.titan.hadoop.compat.HadoopCompatLoader.DEFAULT_COMPAT;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class HadoopFileOutputFormat extends FileOutputFormat<NullWritable, FaunusVertex> {

    protected Configuration faunusConf;

    public DataOutputStream getDataOuputStream(final TaskAttemptContext job) throws IOException, InterruptedException {
        org.apache.hadoop.conf.Configuration hadoopConf = DEFAULT_COMPAT.getContextConfiguration(job);
        this.faunusConf = ModifiableHadoopConfiguration.of(hadoopConf);
        boolean isCompressed = getCompressOutput(job);
        CompressionCodec codec = null;
        String extension = "";
        if (isCompressed) {
            final Class<? extends CompressionCodec> codecClass = getOutputCompressorClass(job, DefaultCodec.class);
            codec = ReflectionUtils.newInstance(codecClass, hadoopConf);
            extension = codec.getDefaultExtension();
        }
        final Path file = super.getDefaultWorkFile(job, extension);
        final FileSystem fs = file.getFileSystem(hadoopConf);
        if (!isCompressed) {
            return new DataOutputStream(fs.create(file, false));
        } else {
            return new DataOutputStream(codec.createOutputStream(fs.create(file, false)));
        }
    }
}