package com.thinkaurelius.titan.hadoop.formats.util;

import com.google.common.collect.ImmutableMap;
import com.thinkaurelius.titan.diskstorage.configuration.ConfigElement;
import com.thinkaurelius.titan.hadoop.config.ModifiableHadoopConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import static com.thinkaurelius.titan.hadoop.config.TitanHadoopConfiguration.OUTPUT_LOADER_SCRIPT_FILE;
import static com.thinkaurelius.titan.hadoop.formats.util.LoaderScriptWrapper.*;

/**
 * A diagnostic tool for Titan-Hadoop incremental loading scripts.
 */
public class LoaderScriptChecker {

    private static final Logger log = LoggerFactory.getLogger(LoaderScriptChecker.class);

    /**
     * Read the Titan-Hadoop configuration file given as the sole argument,
     * then compile the incremental loading script defined in the configuration.
     * Print information about which methods successfully compiled, along with
     * any exceptions encountered during attempted compilation.
     */
    public static void main(String args[]) throws IOException {
        if (null == args || 1 != args.length) {
            log.error("Usage: {} <titan-hadoop-config-file>",
                    LoaderScriptChecker.class.getSimpleName());
            System.exit(2);
        }

        ModifiableHadoopConfiguration faunusConf = getConf(args[0]);

        if (!faunusConf.has(OUTPUT_LOADER_SCRIPT_FILE)) {
            log.error("No value defined to for {} in {}",
                    ConfigElement.getPath(OUTPUT_LOADER_SCRIPT_FILE), args[0]);
            System.exit(2);
        }

        String s = faunusConf.get(OUTPUT_LOADER_SCRIPT_FILE);
        FileSystem fs = FileSystem.get(faunusConf.getHadoopConfiguration());

        log.info("Attempting to load {} from filesystem {}", s, fs);

        LoaderScriptWrapper script = new LoaderScriptWrapper(fs, new Path(s));

        log.info("Summary of methods loaded from {}:", s);

        ImmutableMap<String, Boolean> methods = ImmutableMap.of(
            EDGE_METHOD_NAME, script.hasEdgeMethod(),
            VERTEX_METHOD_NAME, script.hasVertexMethod(),
            VERTEX_PROP_METHOD_NAME, script.hasVPropMethod()
        );

        int ok = 0;
        for (Map.Entry<String, Boolean> m : methods.entrySet()) {
            if (m.getValue()) {
                log.info("Successfully compiled method:       {}", m.getKey());
                ok++;
            } else {
                log.info("Unable to compile or locate method: {}", m.getKey());
            }
        }

        log.info("Detected and compiled {} total method(s) from {}", ok, s);

        System.exit(0 < ok ? 0 : 1);
    }

    private static ModifiableHadoopConfiguration getConf(String path) throws IOException {
        Properties properties = new Properties();
        Configuration configuration = new Configuration();
        properties.load(new FileInputStream(path));
        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
            configuration.set(entry.getKey().toString(), entry.getValue().toString());
        }
        return ModifiableHadoopConfiguration.of(configuration);
    }
}
