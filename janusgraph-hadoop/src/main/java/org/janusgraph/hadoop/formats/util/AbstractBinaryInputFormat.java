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

package org.janusgraph.hadoop.formats.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.HadoopPoolsConfigurable;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.hadoop.config.JanusGraphHadoopConfiguration;
import org.janusgraph.hadoop.config.ModifiableHadoopConfiguration;

public abstract class AbstractBinaryInputFormat extends InputFormat<StaticBuffer, Iterable<Entry>> implements HadoopPoolsConfigurable {

    protected Configuration hadoopConf;
    protected ModifiableHadoopConfiguration mrConf;
    protected ModifiableConfiguration janusgraphConf;

    @Override
    public void setConf(final Configuration config) {
        HadoopPoolsConfigurable.super.setConf(config);
        this.mrConf = ModifiableHadoopConfiguration.of(JanusGraphHadoopConfiguration.MAPRED_NS, config);
        this.hadoopConf = config;
        this.janusgraphConf = mrConf.getJanusGraphConf();
    }

    @Override
    public Configuration getConf() {
        return hadoopConf;
    }
}
