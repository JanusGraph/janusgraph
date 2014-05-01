package com.thinkaurelius.titan.hadoop.tinkerpop.gremlin

import com.thinkaurelius.titan.hadoop.FaunusPipeline
import com.thinkaurelius.titan.hadoop.tinkerpop.gremlin.loaders.GraphLoader
import com.thinkaurelius.titan.hadoop.tinkerpop.gremlin.loaders.HadoopLoader
import com.thinkaurelius.titan.hadoop.tinkerpop.gremlin.loaders.PipeLoader

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
class FaunusGremlin {

    private static final Set<String> steps = new HashSet<String>();

    public static void load() {

        FaunusPipeline.getMethods().each {
            if (it.getReturnType().equals(FaunusPipeline.class)) {
                FaunusGremlin.addStep(it.getName());
            }
        }

        GraphLoader.load();
        PipeLoader.load();
        HadoopLoader.load();
    }

    public static void addStep(final String stepName) {
        FaunusGremlin.steps.add(stepName);
    }

    public static boolean isStep(final String stepName) {
        return FaunusGremlin.steps.contains(stepName);
    }

    public static String version() {
        return "faunus-" + com.thinkaurelius.titan.hadoop.Tokens.VERSION + ":gremlin-" + com.tinkerpop.gremlin.Tokens.VERSION;
    }

    public static String language() {
        return "faunus-gremlin-groovy";
    }
}
