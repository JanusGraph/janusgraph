package org.janusgraph.hadoop.formats.util;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.hadoop.formats.util.input.JanusHadoopSetup;
import org.janusgraph.util.system.ConfigurationUtil;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.VertexWritable;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;
import java.util.List;

import static org.janusgraph.hadoop.formats.util.input.JanusHadoopSetupCommon.SETUP_CLASS_NAME;
import static org.janusgraph.hadoop.formats.util.input.JanusHadoopSetupCommon.SETUP_PACKAGE_PREFIX;

public abstract class GiraphInputFormat extends InputFormat<NullWritable, VertexWritable> implements Configurable {

    private final InputFormat<StaticBuffer, Iterable<Entry>> inputFormat;
    private static final RefCountedCloseable<JanusVertexDeserializer> refCounter;

    static {
        refCounter = new RefCountedCloseable<>((conf) -> {
            final String janusVersion = "current";

            String className = SETUP_PACKAGE_PREFIX + janusVersion + SETUP_CLASS_NAME;

            JanusHadoopSetup ts = ConfigurationUtil.instantiate(
                    className, new Object[]{ conf }, new Class[]{ Configuration.class });

            return new JanusVertexDeserializer(ts);
        });
    }



    public GiraphInputFormat(InputFormat<StaticBuffer, Iterable<Entry>> inputFormat) {
        this.inputFormat = inputFormat;
        Preconditions.checkState(Configurable.class.isAssignableFrom(inputFormat.getClass()));
    }

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
        return inputFormat.getSplits(context);
    }

    @Override
    public RecordReader<NullWritable, VertexWritable> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return new GiraphRecordReader(refCounter, inputFormat.createRecordReader(split, context));
    }

    @Override
    public void setConf(final Configuration conf) {
        ((Configurable)inputFormat).setConf(conf);

        refCounter.setBuilderConfiguration(conf);
    }

    @Override
    public Configuration getConf() {
        return ((Configurable)inputFormat).getConf();
    }

    public static class RefCountedCloseable<T extends AutoCloseable> {

        private T current;
        private long refCount;
        private final Function<Configuration, T> builder;
        private Configuration configuration;

        public RefCountedCloseable(Function<Configuration, T> builder) {
            this.builder = builder;
        }

        public synchronized void setBuilderConfiguration(Configuration configuration) {
            this.configuration = configuration;
        }

        public synchronized T acquire() {
            if (null == current) {
                Preconditions.checkState(0 == refCount);
                current = builder.apply(configuration);
            }

            refCount++;

            return current;
        }

        public synchronized void release() throws Exception {
            Preconditions.checkState(null != current);
            Preconditions.checkState(0 < refCount);

            refCount--;

            if (0 == refCount) {
                current.close();
                current = null;
            }
        }
    }
}
