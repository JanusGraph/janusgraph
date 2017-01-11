// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.hadoop.formats;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.janusgraph.hadoop.config.ModifiableHadoopConfiguration;
import org.janusgraph.hadoop.config.JanusGraphHadoopConfiguration;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class JanusGraphH1OutputFormat extends OutputFormat<NullWritable, VertexWritable> {

    private static final Logger log = LoggerFactory.getLogger(JanusGraphH1OutputFormat.class);

    private final ConcurrentMap<TaskAttemptID, StandardJanusGraphTx> transactions = new ConcurrentHashMap<>();

    private StandardJanusGraph graph;

    private Set<String> persistableKeys;

    @Override
    public RecordWriter<NullWritable, VertexWritable> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {

        synchronized (this) {
            if (null == graph) {
                Configuration hadoopConf = taskAttemptContext.getConfiguration();
                ModifiableHadoopConfiguration mhc =
                        ModifiableHadoopConfiguration.of(JanusGraphHadoopConfiguration.MAPRED_NS, hadoopConf);
                graph = (StandardJanusGraph) JanusGraphFactory.open(mhc.getJanusGraphConf());
            }
        }

        // Special case for a TP3 vertex program: persist only those properties whose keys are
        // returned by VertexProgram.getComputeKeys()
        if (null == persistableKeys) {
            try {
                persistableKeys = VertexProgram.createVertexProgram(graph,
                       ConfUtil.makeApacheConfiguration(taskAttemptContext.getConfiguration())).getElementComputeKeys();
                log.debug("Set persistableKeys={}", Joiner.on(",").join(persistableKeys));
            } catch (Exception e) {
                log.debug("Unable to detect or instantiate vertex program", e);
                persistableKeys = ImmutableSet.of();
            }
        }

        StandardJanusGraphTx tx = transactions.computeIfAbsent(taskAttemptContext.getTaskAttemptID(),
                id -> (StandardJanusGraphTx)graph.newTransaction());
        return new JanusGraphH1RecordWriter(taskAttemptContext, tx, persistableKeys);
    }

    @Override
    public void checkOutputSpecs(JobContext jobContext) throws IOException, InterruptedException {
        // TODO check output configuration for minimum set of keys here?
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext taskAttemptContext) throws IOException,
            InterruptedException {
        return new JanusGraphH1OutputCommitter(this);
    }

    void commit(TaskAttemptID id) {
        StandardJanusGraphTx tx = transactions.remove(id);
        if (null == tx) {
            log.warn("Detected concurrency in task commit");
            return;
        }
        tx.commit();
    }

    void abort(TaskAttemptID id) {
        StandardJanusGraphTx tx = transactions.remove(id);
        if (null == tx) {
            log.warn("Detected concurrency in task abort");
            return;
        }
        tx.rollback();
    }

    boolean hasModifications(TaskAttemptID id) {
        StandardJanusGraphTx tx = transactions.get(id);
        // if tx is null, something is horribly wrong
        return tx.hasModifications();
    }
}
