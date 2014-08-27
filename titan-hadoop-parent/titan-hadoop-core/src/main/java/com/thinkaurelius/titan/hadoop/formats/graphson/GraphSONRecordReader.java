package com.thinkaurelius.titan.hadoop.formats.graphson;

import static com.thinkaurelius.titan.hadoop.compat.HadoopCompatLoader.DEFAULT_COMPAT;

import com.thinkaurelius.titan.diskstorage.configuration.Configuration;
import com.thinkaurelius.titan.hadoop.FaunusVertex;
import com.thinkaurelius.titan.hadoop.config.ModifiableHadoopConfiguration;
import com.thinkaurelius.titan.hadoop.formats.VertexQueryFilter;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GraphSONRecordReader extends RecordReader<NullWritable, FaunusVertex> {

    private final LineRecordReader lineRecordReader;
    private final VertexQueryFilter vertexQuery;
    private FaunusVertex vertex = null;
    private HadoopGraphSONUtility graphsonUtil;

    public GraphSONRecordReader(VertexQueryFilter vertexQuery) {
        lineRecordReader = new LineRecordReader();
        this.vertexQuery = vertexQuery;
    }

    @Override
    public void initialize(final InputSplit genericSplit, final TaskAttemptContext context) throws IOException {
        lineRecordReader.initialize(genericSplit, context);
        org.apache.hadoop.conf.Configuration c = DEFAULT_COMPAT.getContextConfiguration(context);
        Configuration configuration = ModifiableHadoopConfiguration.of(c);
        graphsonUtil = new HadoopGraphSONUtility(configuration);
    }

    @Override
    public boolean nextKeyValue() throws IOException {
        if (!lineRecordReader.nextKeyValue())
            return false;

        vertex = graphsonUtil.fromJSON(lineRecordReader.getCurrentValue().toString());
        vertexQuery.defaultFilter(vertex);
        return true;
    }

    @Override
    public NullWritable getCurrentKey() {
        return NullWritable.get();
    }

    @Override
    public FaunusVertex getCurrentValue() {
        return vertex;
    }

    @Override
    public float getProgress() throws IOException {
        return lineRecordReader.getProgress();
    }

    @Override
    public synchronized void close() throws IOException {
        lineRecordReader.close();
    }
}