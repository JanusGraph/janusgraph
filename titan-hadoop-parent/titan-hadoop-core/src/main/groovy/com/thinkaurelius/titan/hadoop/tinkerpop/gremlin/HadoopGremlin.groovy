package com.thinkaurelius.titan.hadoop.tinkerpop.gremlin

import com.thinkaurelius.titan.hadoop.HadoopPipeline
import com.thinkaurelius.titan.hadoop.tinkerpop.gremlin.loaders.GraphLoader
import com.thinkaurelius.titan.hadoop.tinkerpop.gremlin.loaders.HadoopLoader
import com.thinkaurelius.titan.hadoop.tinkerpop.gremlin.loaders.PipeLoader

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
class HadoopGremlin {

    private static final Set<String> steps = new HashSet<String>();

    public static void load() {

        HadoopPipeline.getMethods().each {
            if (it.getReturnType().equals(HadoopPipeline.class)) {
                HadoopGremlin.addStep(it.getName());
            }
        }

        GraphLoader.load();
        PipeLoader.load();
        HadoopLoader.load();
    }

    public static void addStep(final String stepName) {
        HadoopGremlin.steps.add(stepName);
    }

    public static boolean isStep(final String stepName) {
        return HadoopGremlin.steps.contains(stepName);
    }

    public static String version() {
        return "titan-hadoop-" + com.thinkaurelius.titan.graphdb.configuration.TitanConstants.VERSION + ":gremlin-" + com.tinkerpop.gremlin.Tokens.VERSION;
    }

    public static String language() {
        return "hadoop-gremlin-groovy";
    }
}
