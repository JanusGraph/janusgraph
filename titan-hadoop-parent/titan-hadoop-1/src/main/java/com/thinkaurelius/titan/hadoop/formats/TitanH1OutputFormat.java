package com.thinkaurelius.titan.hadoop.formats;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.thinkaurelius.titan.hadoop.config.ModifiableHadoopConfiguration;
import com.thinkaurelius.titan.hadoop.config.TitanHadoopConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.VertexWritable;
import org.apache.tinkerpop.gremlin.hadoop.structure.util.ConfUtil;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.VertexComputeKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Set;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TitanH1OutputFormat extends OutputFormat<NullWritable, VertexWritable> {

    private static final Logger log = LoggerFactory.getLogger(TitanH1OutputFormat.class);

    private final ConcurrentMap<TaskAttemptID, StandardTitanTx> transactions = new ConcurrentHashMap<>();

    private StandardTitanGraph graph;

    private Set<String> persistableKeys;

    @Override
    public RecordWriter<NullWritable, VertexWritable> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {

        synchronized (this) {
            if (null == graph) {
                Configuration hadoopConf = taskAttemptContext.getConfiguration();
                ModifiableHadoopConfiguration mhc =
                        ModifiableHadoopConfiguration.of(TitanHadoopConfiguration.MAPRED_NS, hadoopConf);
                graph = (StandardTitanGraph) TitanFactory.open(mhc.getTitanGraphConf());
            }
        }

        // Special case for a TP3 vertex program: persist only those properties whose keys are
        // returned by VertexProgram.getComputeKeys()
        if (null == persistableKeys) {
            try {
		Stream<VertexComputeKey> persistableKeysStream = VertexProgram.createVertexProgram(graph, ConfUtil.makeApacheConfiguration(taskAttemptContext.getConfiguration())).getVertexComputeKeys().stream();
                persistableKeys = persistableKeysStream.map( k -> k.getKey()).collect(Collectors.toCollection(HashSet::new));
                log.debug("Set persistableKeys={}", Joiner.on(",").join(persistableKeys));
            } catch (Exception e) {
                log.debug("Unable to detect or instantiate vertex program", e);
                persistableKeys = ImmutableSet.of();
            }
        }

        StandardTitanTx tx = transactions.computeIfAbsent(taskAttemptContext.getTaskAttemptID(),
                id -> (StandardTitanTx)graph.newTransaction());
        return new TitanH1RecordWriter(taskAttemptContext, tx, persistableKeys);
    }

    @Override
    public void checkOutputSpecs(JobContext jobContext) throws IOException, InterruptedException {
        // TODO check output configuration for minimum set of keys here?
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext taskAttemptContext) throws IOException,
            InterruptedException {
        return new TitanH1OutputCommitter(this);
    }

    void commit(TaskAttemptID id) {
        StandardTitanTx tx = transactions.remove(id);
        if (null == tx) {
            log.warn("Detected concurrency in task commit");
            return;
        }
        tx.commit();
    }

    void abort(TaskAttemptID id) {
        StandardTitanTx tx = transactions.remove(id);
        if (null == tx) {
            log.warn("Detected concurrency in task abort");
            return;
        }
        tx.rollback();
    }

    boolean hasModifications(TaskAttemptID id) {
        StandardTitanTx tx = transactions.get(id);
        // if tx is null, something is horribly wrong
        return tx.hasModifications();
    }
}
