package com.thinkaurelius.titan.hadoop.tinkerpop.gremlin.loaders

import com.thinkaurelius.titan.hadoop.HadoopGraph
import com.thinkaurelius.titan.hadoop.HadoopPipeline
import com.thinkaurelius.titan.hadoop.tinkerpop.gremlin.HadoopGremlin

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
class GraphLoader {

    private static final String V = "V";
    private static final String E = "E";

    public static void load() {

        HadoopGraph.metaClass.propertyMissing = { final String name ->
            if (name.equals(V)) {
                return new HadoopPipeline((HadoopGraph) delegate).V();
            } else if (name.equals(E)) {
                return new HadoopPipeline((HadoopGraph) delegate).E();
            } else if (HadoopGremlin.isStep(name)) {
                return new HadoopPipeline((HadoopGraph) delegate)."$name"();
            } else {
                throw new MissingPropertyException(name, delegate.getClass());
            }
        }

        HadoopGraph.metaClass.methodMissing = { final String name, final def args ->
            if (HadoopGremlin.isStep(name)) {
                return new HadoopPipeline((HadoopGraph) delegate)."$name"(* args);
            } else {
                throw new MissingMethodException(name, delegate.getClass());
            }
        }

        HadoopGraph.metaClass.v = { final long ... ids ->
            return new HadoopPipeline((HadoopGraph) delegate).v(ids);
        }

        HadoopGraph.metaClass.V = {->
            return new HadoopPipeline((HadoopGraph) delegate).V();
        }

        HadoopGraph.metaClass.V = { final String key, final Object value ->
            return new HadoopPipeline((HadoopGraph) delegate).V().has(key, value);
        }

        HadoopGraph.metaClass.E = {->
            return new HadoopPipeline((HadoopGraph) delegate).E();
        }

        HadoopGraph.metaClass.E = { final String key, final Object value ->
            return new HadoopPipeline((HadoopGraph) delegate).E().has(key, value);
        }

        HadoopGraph.metaClass._ = {->
            return new HadoopPipeline((HadoopGraph) delegate)._();
        }
    }
}
