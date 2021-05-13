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

package org.janusgraph.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.janusgraph.diskstorage.configuration.BasicConfiguration;
import org.janusgraph.diskstorage.configuration.ConfigElement;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.configuration.backend.CommonsConfiguration;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.hadoop.config.JanusGraphHadoopConfiguration;
import org.janusgraph.util.system.ConfigurationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;

public class MapReduceIndexJobs {

    private static final Logger log =
            LoggerFactory.getLogger(MapReduceIndexJobs.class);

    public static ModifiableConfiguration getIndexJobConf(String indexName, String relationType) {
        ModifiableConfiguration mc = new ModifiableConfiguration(GraphDatabaseConfiguration.JOB_NS,
                new CommonsConfiguration(ConfigurationUtil.createBaseConfiguration()), BasicConfiguration.Restriction.NONE);
        mc.set(org.janusgraph.graphdb.olap.job.IndexUpdateJob.INDEX_NAME, indexName);
        mc.set(org.janusgraph.graphdb.olap.job.IndexUpdateJob.INDEX_RELATION_TYPE, relationType);
        mc.set(GraphDatabaseConfiguration.JOB_START_TIME, System.currentTimeMillis());
        return mc;
    }

    public static void copyPropertiesToInputAndOutputConf(Configuration sink, Properties source) {
        final String prefix = ConfigElement.getPath(JanusGraphHadoopConfiguration.GRAPH_CONFIG_KEYS, true) + ".";
        for (Map.Entry<Object, Object> e : source.entrySet()) {
            String k;
            String v = e.getValue().toString();
            k = prefix + e.getKey().toString();
            sink.set(k, v);
            log.info("Set {}={}", k, v);
        }
    }
}
