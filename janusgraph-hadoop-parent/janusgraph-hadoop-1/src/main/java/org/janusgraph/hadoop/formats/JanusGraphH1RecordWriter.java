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

import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
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

public class JanusGraphH1RecordWriter extends RecordWriter<NullWritable, VertexWritable> {

    private static final Logger log = LoggerFactory.getLogger(JanusGraphH1RecordWriter.class);

    private final TaskAttemptContext taskAttemptContext;
    private final StandardJanusGraphTx tx;
    private final Set<String> persistableKeys;

    public JanusGraphH1RecordWriter(TaskAttemptContext taskAttemptContext, StandardJanusGraphTx tx, Set<String> persistableKeys) {
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
        // commit/rollback happens in JanusGraphOutputCommitter
        // nothing to do here
    }
}
