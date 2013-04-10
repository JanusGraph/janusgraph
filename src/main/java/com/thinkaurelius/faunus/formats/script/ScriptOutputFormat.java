package com.thinkaurelius.faunus.formats.script;

import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.formats.FaunusFileOutputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ScriptOutputFormat extends FaunusFileOutputFormat {

    public static final String OUTPUT_SCRIPT_FILE = "faunus.output.script.file";

    @Override
    public RecordWriter<NullWritable, FaunusVertex> getRecordWriter(final TaskAttemptContext job) throws IOException, InterruptedException {
        return new ScriptRecordWriter(super.getDataOuputStream(job), job.getConfiguration());
    }
}