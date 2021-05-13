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

package org.janusgraph.hadoop.serialize;

import com.google.common.collect.ImmutableList;
import org.apache.commons.configuration2.BaseConfiguration;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.HadoopPoolShimService;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.HadoopPools;
import org.apache.tinkerpop.gremlin.structure.io.IoRegistry;
import org.janusgraph.graphdb.tinkerpop.JanusGraphIoRegistry;
import org.janusgraph.util.system.ConfigurationUtil;

public class JanusGraphKryoShimService extends HadoopPoolShimService {

    public JanusGraphKryoShimService() {
        final BaseConfiguration c = ConfigurationUtil.createBaseConfiguration();
        c.setProperty(IoRegistry.IO_REGISTRY, ImmutableList.of(JanusGraphIoRegistry.class.getCanonicalName()));
        HadoopPools.initialize(c);
    }

}
