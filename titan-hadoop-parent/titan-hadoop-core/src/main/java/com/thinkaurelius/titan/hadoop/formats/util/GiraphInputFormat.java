package com.thinkaurelius.titan.hadoop.formats.util;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.Entry;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.hadoop.formats.util.input.TitanHadoopSetup;
import com.thinkaurelius.titan.util.system.ConfigurationUtil;
import com.tinkerpop.gremlin.giraph.process.computer.GiraphComputeVertex;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;
import java.util.List;

import static com.thinkaurelius.titan.hadoop.formats.util.input.TitanHadoopSetupCommon.SETUP_CLASS_NAME;
import static com.thinkaurelius.titan.hadoop.formats.util.input.TitanHadoopSetupCommon.SETUP_PACKAGE_PREFIX;

public abstract class GiraphInputFormat extends InputFormat<NullWritable, GiraphComputeVertex> implements Configurable {

    private final InputFormat<StaticBuffer, Iterable<Entry>> inputFormat;
    private TitanVertexDeserializer vertexDeserializer;

    public GiraphInputFormat(InputFormat<StaticBuffer, Iterable<Entry>> inputFormat) {
        this.inputFormat = inputFormat;
        Preconditions.checkState(Configurable.class.isAssignableFrom(inputFormat.getClass()));
    }

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
        return inputFormat.getSplits(context);
    }

    @Override
    public RecordReader<NullWritable, GiraphComputeVertex> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return new GiraphRecordReader(getVertexDeserializer(), inputFormat.createRecordReader(split, context));
    }

    @Override
    public void setConf(Configuration conf) {
        ((Configurable)inputFormat).setConf(conf);

        final String titanVersion = "current";
        String className = SETUP_PACKAGE_PREFIX + titanVersion + SETUP_CLASS_NAME;
        TitanHadoopSetup ts = ConfigurationUtil.instantiate(className, new Object[]{conf}, new Class[]{Configuration.class});
        vertexDeserializer = new TitanVertexDeserializer(ts);
    }

    @Override
    public Configuration getConf() {
        return ((Configurable)inputFormat).getConf();
    }

    public TitanVertexDeserializer getVertexDeserializer() {
        return vertexDeserializer;
    }
}
