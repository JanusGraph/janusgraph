package com.thinkaurelius.faunus.formats.sequence;

import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.formats.SchemaTools;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class FaunusSequenceFileOutputFormat extends SequenceFileOutputFormat<NullWritable, FaunusVertex> {

    public RecordWriter<NullWritable, FaunusVertex> getRecordWriter(final TaskAttemptContext context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();

        // write the schema to the output directory
        SchemaTools.writeSchema(configuration);

        CompressionCodec codec = null;
        SequenceFile.CompressionType compressionType = SequenceFile.CompressionType.NONE;
        if (getCompressOutput(context)) {
            // find the kind of compression to do
            compressionType = getOutputCompressionType(context);
            // find the right codec
            Class<?> codecClass = getOutputCompressorClass(context, DefaultCodec.class);
            codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, configuration);
        }
        // get the path of the temporary output file
        Path file = getDefaultWorkFile(context, "");
        FileSystem fs = file.getFileSystem(configuration);

        final SequenceFile.Writer out = SequenceFile.createWriter(fs, configuration, file,
                context.getOutputKeyClass(),
                context.getOutputValueClass(),
                compressionType,
                codec,
                context);

        return new FaunusSequenceFileRecordWriter(out);
    }
}
