package com.thinkaurelius.titan.hadoop.tinkerpop.gremlin.loaders

import com.thinkaurelius.titan.hadoop.FaunusPipeline
import com.thinkaurelius.titan.hadoop.tinkerpop.gremlin.FaunusGremlin

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
class PipeLoader {

    public static void load() {
        FaunusPipeline.metaClass.propertyMissing = { final String name ->
            if (FaunusGremlin.isStep(name)) {
                return delegate."$name"();
            } else {
                FaunusPipeline.metaClass."$name" = { ((FaunusPipeline) delegate).property(name); }
                return ((FaunusPipeline) delegate).property(name);
            }
        }
    }
}
