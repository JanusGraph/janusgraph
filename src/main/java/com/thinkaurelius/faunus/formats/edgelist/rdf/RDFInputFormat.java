package com.thinkaurelius.faunus.formats.edgelist.rdf;

import com.thinkaurelius.faunus.FaunusElement;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class RDFInputFormat extends FileInputFormat<NullWritable, FaunusElement> {

    public static final String RDF_FORMAT = "faunus.input.rdf.format";
    public static final String USE_LOCALNAME = "faunus.input.rdf.use-localname";
    public static final String AS_PROPERTIES = "faunus.input.rdf.as-properties";
    public static final String LITERAL_AS_PROPERTY = "faunus.input.rdf.literal-as-property";

    public static final String URI = "uri";
    public static final String CONTEXT = "context";
    public static final String NAME = "name";

    @Override
    public RecordReader<NullWritable, FaunusElement> createRecordReader(final InputSplit split, final TaskAttemptContext context) {
        return new RDFRecordReader();
    }

    @Override
    protected boolean isSplitable(final JobContext context, final Path file) {
        return null == new CompressionCodecFactory(context.getConfiguration()).getCodec(file);
    }

}