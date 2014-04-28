package com.thinkaurelius.faunus.tinkerpop.gremlin.loaders

import com.thinkaurelius.faunus.FaunusGraph
import com.thinkaurelius.faunus.FaunusPipeline
import com.thinkaurelius.faunus.tinkerpop.gremlin.FaunusGremlin

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
class GraphLoader {

    private static final String V = "V";
    private static final String E = "E";

    public static void load() {

        FaunusGraph.metaClass.propertyMissing = { final String name ->
            if (name.equals(V)) {
                return new FaunusPipeline((FaunusGraph) delegate).V();
            } else if (name.equals(E)) {
                return new FaunusPipeline((FaunusGraph) delegate).E();
            } else if (FaunusGremlin.isStep(name)) {
                return new FaunusPipeline((FaunusGraph) delegate)."$name"();
            } else {
                throw new MissingPropertyException(name, delegate.getClass());
            }
        }

        FaunusGraph.metaClass.methodMissing = { final String name, final def args ->
            if (FaunusGremlin.isStep(name)) {
                return new FaunusPipeline((FaunusGraph) delegate)."$name"(* args);
            } else {
                throw new MissingMethodException(name, delegate.getClass());
            }
        }

        FaunusGraph.metaClass.v = { final long ... ids ->
            return new FaunusPipeline((FaunusGraph) delegate).v(ids);
        }

        FaunusGraph.metaClass.V = {->
            return new FaunusPipeline((FaunusGraph) delegate).V();
        }

        FaunusGraph.metaClass.V = { final String key, final Object value ->
            return new FaunusPipeline((FaunusGraph) delegate).V().has(key, value);
        }

        FaunusGraph.metaClass.E = {->
            return new FaunusPipeline((FaunusGraph) delegate).E();
        }

        FaunusGraph.metaClass.E = { final String key, final Object value ->
            return new FaunusPipeline((FaunusGraph) delegate).E().has(key, value);
        }

        FaunusGraph.metaClass._ = {->
            return new FaunusPipeline((FaunusGraph) delegate)._();
        }
    }
}
