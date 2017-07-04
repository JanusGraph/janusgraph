package org.janusgraph.hadoop.serialize;

import com.google.common.collect.ImmutableList;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.HadoopPoolShimService;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.HadoopPools;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoPool;
import org.janusgraph.graphdb.tinkerpop.JanusGraphIoRegistry;

public class JanusGraphKryoShimService extends HadoopPoolShimService {

    public JanusGraphKryoShimService() {
        final BaseConfiguration c = new BaseConfiguration();
        c.setProperty(GryoPool.CONFIG_IO_REGISTRY, ImmutableList.of(JanusGraphIoRegistry.class.getCanonicalName()));
        HadoopPools.initialize(c);
    }

}
