package com.thinkaurelius.faunus.tinkerpop.rexster;

import com.thinkaurelius.faunus.FaunusGraph;
import com.thinkaurelius.faunus.FaunusPipeline;
import com.thinkaurelius.faunus.tinkerpop.gremlin.FaunusGremlinScriptEngine;
import com.tinkerpop.rexster.RexsterApplicationGraph;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import java.util.Iterator;
import java.util.Map;
import java.util.UUID;

/**
 * Responsible for executing a Faunus job as triggered from the FaunusRexsterExecutorExtension.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class FaunusEvaluationJob {
    private static final Logger logger = Logger.getLogger(FaunusEvaluationJob.class);
    protected final String script;
    protected volatile boolean complete = false;
    protected volatile boolean error = false;
    private volatile String errorMessage = "";
    private UUID jobId;
    private final RexsterApplicationGraph rag;
    private final Map<String,String> configOverrides;

    public FaunusEvaluationJob(final String script, final UUID jobId, final RexsterApplicationGraph rag,
                               final Map<String,String> configOverrides) {
        this.script = script;
        this.jobId = jobId;
        this.rag = rag;
        this.configOverrides = configOverrides;
    }

    public boolean isComplete() {
        return complete;
    }

    public boolean isError() {
        return error;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void performJob() {
        try {
            logger.info(String.format("Submit Faunus job [%s]", jobId));
            ((FaunusPipeline) getScriptEngine(rag, configOverrides).eval(script)).submit();
        } catch (Exception ex) {
            logger.error(String.format("Error running Faunus job [%s].", jobId), ex);
            if (ex.getCause() != null) {
                logger.error("Error details", ex.getCause());
            }

            synchronized (this) {
                error = true;
                errorMessage = ex.getMessage();
            }
        } finally {
            complete = true;
        }
    }

    private static FaunusGremlinScriptEngine getScriptEngine(final RexsterApplicationGraph rag,
                                                             final Map<String,String> configOverrides) {
        final FaunusGremlinScriptEngine engine = new FaunusGremlinScriptEngine();
        final Configuration configuration = new Configuration();

        final Map<String,String> properties = rag.findExtensionConfiguration(
                FaunusRexsterExecutorExtension.EXTENSION_NAMESPACE, FaunusRexsterExecutorExtension.EXTENSION_NAME)
                .tryGetMapFromConfiguration();

        // add properties from rexster.xml to the configuration
        applyToConfiguration(configuration, properties);

        // override rexster.xml values with those from the job request
        applyToConfiguration(configuration, configOverrides);

        writeConfigurationToLog(configuration);

        final FaunusGraph graph = new FaunusGraph(configuration);
        engine.put("g", graph);
        return engine;
    }

    private static void applyToConfiguration(final Configuration configuration, final Map<String, String> properties) {
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            // replace the ".." put in play by apache commons configuration.  that's expected behavior
            // due to parsing key names to xml.
            final String key = entry.getKey().replace("..", ".");
            configuration.set(key, entry.getValue());
        }
    }

    private static void writeConfigurationToLog(final Configuration conf) {
        final StringBuilder sb = new StringBuilder();
        sb.append("Faunus settings:\n");

        final Iterator itty = conf.iterator();
        while (itty.hasNext()) {
            final Map.Entry<String,String> e = (Map.Entry<String,String>) itty.next();

            sb.append(e.getKey());
            sb.append("=");
            sb.append(e.getValue());
            sb.append("\n");
        }

        final String configurationToPrint = sb.toString();
        logger.info(configurationToPrint.substring(0, configurationToPrint.length() - 1));
    }
}
