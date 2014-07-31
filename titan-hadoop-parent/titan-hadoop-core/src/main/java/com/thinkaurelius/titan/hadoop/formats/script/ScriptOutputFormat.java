package com.thinkaurelius.titan.hadoop.formats.script;

import com.thinkaurelius.titan.hadoop.FaunusVertex;
import com.thinkaurelius.titan.hadoop.formats.HadoopFileOutputFormat;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

import static com.thinkaurelius.titan.hadoop.compat.HadoopCompatLoader.DEFAULT_COMPAT;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ScriptOutputFormat extends HadoopFileOutputFormat {

    @Override
    public RecordWriter<NullWritable, FaunusVertex> getRecordWriter(final TaskAttemptContext job) throws IOException, InterruptedException {
        return new ScriptRecordWriter(super.getDataOuputStream(job), DEFAULT_COMPAT.getContextConfiguration(job));
    }
}