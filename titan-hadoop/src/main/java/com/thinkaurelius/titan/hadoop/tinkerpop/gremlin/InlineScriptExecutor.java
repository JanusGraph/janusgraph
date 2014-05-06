package com.thinkaurelius.titan.hadoop.tinkerpop.gremlin;

import com.thinkaurelius.titan.hadoop.HadoopGraph;
import com.thinkaurelius.titan.hadoop.HadoopPipeline;

import org.apache.hadoop.conf.Configuration;

import java.io.FileInputStream;
import java.util.Map;
import java.util.Properties;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class InlineScriptExecutor {

    public static void main(final String[] args) throws Exception {
        if (args.length < 1 || args.length > 3 || (args.length == 1 && args[0].contains("-h"))) {
            System.out.println("Faunus Usage:");
            System.out.println("  arg1: Faunus graph configuration file (optional): defaults to bin/titan-hadoop.properties");
            System.out.println("  arg2: HadoopGremlin script: 'g.V.step.step...'");
            System.out.println("  arg3: Overriding configurations (optional): '-Dmapred.map.tasks=14 mapred.reduce.tasks=6'");
            System.exit(-1);
        }

        final String script;
        final String file;
        final Properties fileConfiguration = new Properties();
        final Properties commandLineConfiguration = new Properties();
        if (args.length == 1) {
            script = args[0];
            file = "bin/titan-hadoop.properties";
        } else if (args.length == 2) {
            if (args[1].startsWith("-D")) {
                script = args[0];
                file = "bin/titan-hadoop.properties";
                for (final String property : args[1].substring(2).trim().split(" ")) {
                    commandLineConfiguration.put(property.split("=")[0], property.split("=")[1]);
                }
            } else {
                file = args[0];
                script = args[1];
            }
        } else {
            file = args[0];
            script = args[1];
            for (final String property : args[2].substring(2).trim().split(" ")) {
                commandLineConfiguration.put(property.split("=")[0], property.split("=")[1]);
            }
        }

        fileConfiguration.load(new FileInputStream(file));


        final Configuration conf = new Configuration();
        for (Map.Entry<Object, Object> entry : fileConfiguration.entrySet()) {
            conf.set(entry.getKey().toString(), entry.getValue().toString());
        }
        for (Map.Entry<Object, Object> entry : commandLineConfiguration.entrySet()) {
            conf.set(entry.getKey().toString(), entry.getValue().toString());
        }

        final FaunusGremlinScriptEngine scriptEngine = new FaunusGremlinScriptEngine();
        scriptEngine.put("g", new HadoopGraph(conf));
        Object result = scriptEngine.eval(script);
        if (result.getClass().equals(HadoopPipeline.class))
            ((HadoopPipeline) result).submit(script, true);
        System.exit(0);
    }
}
