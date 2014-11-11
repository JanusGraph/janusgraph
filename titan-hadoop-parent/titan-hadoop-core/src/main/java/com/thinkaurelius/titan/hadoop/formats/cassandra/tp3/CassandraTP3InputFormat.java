package com.thinkaurelius.titan.hadoop.formats.cassandra.tp3;

import com.thinkaurelius.titan.diskstorage.Entry;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.hadoop.config.ModifiableHadoopConfiguration;
import com.thinkaurelius.titan.hadoop.formats.cassandra.CassandraBinaryInputFormat;
import com.thinkaurelius.titan.hadoop.formats.util.TitanVertexDeserializer;
import com.thinkaurelius.titan.hadoop.formats.util.input.TitanHadoopSetup;
import com.thinkaurelius.titan.util.system.ConfigurationUtil;
import com.tinkerpop.gremlin.giraph.process.computer.GiraphComputeVertex;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;
import java.util.List;

import static com.thinkaurelius.titan.hadoop.config.TitanHadoopConfiguration.TITAN_INPUT_VERSION;

import static com.thinkaurelius.titan.hadoop.formats.util.input.TitanHadoopSetupCommon.*;

public class CassandraTP3InputFormat extends InputFormat<NullWritable, GiraphComputeVertex> implements Configurable {

    private final CassandraBinaryInputFormat inputFormat = new CassandraBinaryInputFormat();
    private TitanVertexDeserializer vertexDeserializer;

    public CassandraTP3InputFormat() {
    }

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
        return inputFormat.getSplits(context);
    }

    @Override
    public RecordReader<NullWritable, GiraphComputeVertex> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return new CassandraTP3RecordReader(getVertexDeserializer(), inputFormat.createRecordReader(split, context));
    }

    @Override
    public void setConf(Configuration conf) {
        inputFormat.setConf(conf);

        String titanVersion = ModifiableHadoopConfiguration.of(conf).get(TITAN_INPUT_VERSION);
        String className = SETUP_PACKAGE_PREFIX + titanVersion + SETUP_CLASS_NAME;
        TitanHadoopSetup ts = ConfigurationUtil.instantiate(className, new Object[]{conf}, new Class[]{Configuration.class});
        vertexDeserializer = new TitanVertexDeserializer(ts);
    }

    @Override
    public Configuration getConf() {
        return inputFormat.getConf();
    }

    public TitanVertexDeserializer getVertexDeserializer() {
        return vertexDeserializer;
    }

    public RecordReader<StaticBuffer, Iterable<Entry>> getBinaryRecordReader() {
        return inputFormat.getRecordReader();
    }
}
