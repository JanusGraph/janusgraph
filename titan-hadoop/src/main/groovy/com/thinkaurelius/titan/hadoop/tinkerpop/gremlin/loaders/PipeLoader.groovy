package com.thinkaurelius.titan.hadoop.tinkerpop.gremlin.loaders

import com.thinkaurelius.titan.hadoop.HadoopPipeline
import com.thinkaurelius.titan.hadoop.tinkerpop.gremlin.HadoopGremlin

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
class PipeLoader {

    public static void load() {
        HadoopPipeline.metaClass.propertyMissing = { final String name ->
            if (HadoopGremlin.isStep(name)) {
                return delegate."$name"();
            } else {
                HadoopPipeline.metaClass."$name" = { ((HadoopPipeline) delegate).property(name); }
                return ((HadoopPipeline) delegate).property(name);
            }
        }
    }
}
