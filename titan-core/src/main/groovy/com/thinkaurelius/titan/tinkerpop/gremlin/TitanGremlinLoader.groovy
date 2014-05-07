package com.thinkaurelius.titan.tinkerpop.gremlin

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
class TitanGremlinLoader {

    public static load() {
        try {
            //HadoopGremlin.load();
        } catch (Exception e) {
            // do nothing as the loaders desired are not in the classpath
        }
    }
}
