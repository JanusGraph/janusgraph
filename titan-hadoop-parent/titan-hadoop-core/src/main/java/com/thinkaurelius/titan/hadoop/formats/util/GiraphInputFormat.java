package com.thinkaurelius.titan.hadoop.formats.util;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.Entry;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.hadoop.formats.util.input.TitanHadoopSetup;
import com.thinkaurelius.titan.util.system.ConfigurationUtil;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.VertexWritable;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;
import java.util.List;
import java.util.function.Supplier;

import static com.thinkaurelius.titan.hadoop.formats.util.input.TitanHadoopSetupCommon.SETUP_CLASS_NAME;
import static com.thinkaurelius.titan.hadoop.formats.util.input.TitanHadoopSetupCommon.SETUP_PACKAGE_PREFIX;

public abstract class GiraphInputFormat extends InputFormat<NullWritable, VertexWritable> implements Configurable {

    private final InputFormat<StaticBuffer, Iterable<Entry>> inputFormat;
    private RefCountedCloseable<TitanVertexDeserializer> refCounter;

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
    public void setConf(Configuration conf) {
        ((Configurable)inputFormat).setConf(conf);
        refCounter = new RefCountedCloseable<>(() -> {
            final String titanVersion = "current";
            String className = SETUP_PACKAGE_PREFIX + titanVersion + SETUP_CLASS_NAME;
            TitanHadoopSetup ts = ConfigurationUtil.instantiate(className, new Object[]{conf}, new Class[]{Configuration.class});
            return new TitanVertexDeserializer(ts);
        });
    }

    @Override
    public Configuration getConf() {
        return ((Configurable)inputFormat).getConf();
    }

    public static class RefCountedCloseable<T extends AutoCloseable> {

        private T current;
        private long refCount;
        private final Supplier<T> builder;

        public RefCountedCloseable(Supplier<T> builder) {
            this.builder = builder;
        }

        public synchronized T acquire() {
            if (null == current) {
                Preconditions.checkState(0 == refCount);
                current = builder.get();
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
