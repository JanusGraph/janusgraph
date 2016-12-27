package com.thinkaurelius.titan.hadoop.formats;

import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.VertexWritable;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;

public class TitanH1RecordWriter extends RecordWriter<NullWritable, VertexWritable> {

    private static final Logger log = LoggerFactory.getLogger(TitanH1RecordWriter.class);

    private final TaskAttemptContext taskAttemptContext;
    private final StandardTitanTx tx;
    private final Set<String> persistableKeys;

    public TitanH1RecordWriter(TaskAttemptContext taskAttemptContext, StandardTitanTx tx, Set<String> persistableKeys) {
        this.taskAttemptContext = taskAttemptContext;
        this.tx = tx;
        this.persistableKeys = persistableKeys;
    }

    @Override
    public void write(NullWritable key, VertexWritable value) throws IOException, InterruptedException {
        // TODO tolerate possibility that concurrent OLTP activity has deleted the vertex?  maybe configurable...
        Object vertexID = value.get().id();
        Vertex vertex = tx.vertices(vertexID).next();
        Iterator<VertexProperty<Object>> vpIter = value.get().properties();
        while (vpIter.hasNext()) {
            VertexProperty<Object> vp = vpIter.next();
            if (!persistableKeys.isEmpty() && !persistableKeys.contains(vp.key())) {
                log.debug("[vid {}] skipping key {}", vertexID, vp.key());
                continue;
            }
            vertex.property(vp.key(), vp.value());
            log.debug("[vid {}] set {}={}", vertexID, vp.key(), vp.value());
        }
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        // commit/rollback happens in TitanOutputCommitter
        // nothing to do here
    }
}
