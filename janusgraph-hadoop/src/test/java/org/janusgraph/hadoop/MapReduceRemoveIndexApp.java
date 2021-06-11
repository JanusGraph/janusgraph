// Copyright 2021 JanusGraph Authors
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

package org.janusgraph.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.core.schema.SchemaAction;
import org.janusgraph.diskstorage.keycolumnvalue.scan.ScanMetrics;

/**
 * This class demonstrates how Apache ToolRunner can be used to launch a MapReduce job for index management.
 */
public class MapReduceRemoveIndexApp extends Configured implements Tool {
    private JanusGraph graph;
    private String indexName;
    private ScanMetrics metrics;

    public MapReduceRemoveIndexApp(JanusGraph graph, String indexName) {
       this.graph = graph;
       this.indexName = indexName;
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration hadoopConf = getConf();
        MapReduceIndexManagement mr = new MapReduceIndexManagement(graph);
        JanusGraphManagement mgmt = graph.openManagement();
        metrics = mr.updateIndex(mgmt.getGraphIndex(indexName), SchemaAction.REMOVE_INDEX, hadoopConf).get();
        return 0;
    }

    public ScanMetrics getMetrics() {
        return this.metrics;
    }
}
