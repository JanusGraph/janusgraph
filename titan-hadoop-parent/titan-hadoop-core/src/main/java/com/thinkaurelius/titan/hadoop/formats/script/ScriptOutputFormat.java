package com.thinkaurelius.titan.hadoop.formats.script;

import com.thinkaurelius.titan.hadoop.HadoopVertex;
import com.thinkaurelius.titan.hadoop.formats.HadoopFileOutputFormat;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ScriptOutputFormat extends HadoopFileOutputFormat {

    public static final String TITAN_HADOOP_GRAPH_OUTPUT_SCRIPT_FILE = "titan.hadoop.output.script.file";

    @Override
    public RecordWriter<NullWritable, HadoopVertex> getRecordWriter(final TaskAttemptContext job) throws IOException, InterruptedException {
        return new ScriptRecordWriter(super.getDataOuputStream(job), job.getConfiguration());
    }
}