package com.thinkaurelius.faunus.tinkerpop.rexster;

import com.tinkerpop.rexster.RexsterResourceContext;
import com.tinkerpop.rexster.extension.AbstractRexsterExtension;
import com.tinkerpop.rexster.extension.ExtensionDefinition;
import com.tinkerpop.rexster.extension.ExtensionDescriptor;
import com.tinkerpop.rexster.extension.ExtensionMethod;
import com.tinkerpop.rexster.extension.ExtensionNaming;
import com.tinkerpop.rexster.extension.ExtensionPoint;
import com.tinkerpop.rexster.extension.ExtensionResponse;
import com.tinkerpop.rexster.extension.HttpMethod;
import com.tinkerpop.rexster.extension.RexsterContext;
import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONObject;

import javax.ws.rs.core.Response;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Allows for remote execution and monitoring of a Faunus job via Rexster.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@ExtensionNaming(namespace = FaunusRexsterExecutorExtension.EXTENSION_NAMESPACE, name = FaunusRexsterExecutorExtension.EXTENSION_NAME)
public class FaunusRexsterExecutorExtension extends AbstractRexsterExtension {
    private static final Logger logger = Logger.getLogger(FaunusRexsterExecutorExtension.class);

    public static final String EXTENSION_NAMESPACE = "faunus";
    public static final String EXTENSION_NAME = "executor";

    public static final String STATUS_ERROR = "error";
    public static final String STATUS_PROCESSING = "processing";
    public static final String STATUS_COMPLETE = "complete";

    public static final String FIELD_JOB = "job";
    public static final String FIELD_STATUS = "status";
    public static final String FIELD_MESSAGE = "message";

    private static final ConcurrentMap<String,FaunusEvaluationJob> jobs = new ConcurrentHashMap<String, FaunusEvaluationJob>();

    @ExtensionDefinition(extensionPoint = ExtensionPoint.GRAPH, method = HttpMethod.POST)
    @ExtensionDescriptor(description = "Execute a Faunus job")
    public ExtensionResponse evaluatePost(@RexsterContext final RexsterResourceContext context) {
        final String script = context.getRequestObject().optString("script");
        if (script == null || script.isEmpty()) {
            ExtensionMethod extMethod = context.getExtensionMethod();
            return ExtensionResponse.error("the script parameter cannot be empty", null,
                    Response.Status.BAD_REQUEST.getStatusCode(), null, generateErrorJson(extMethod.getExtensionApiAsJson()));
        }

        final JSONObject config = context.getRequestObject().optJSONObject("config");
        final UUID jobId = UUID.randomUUID();
        final FaunusEvaluationJob job = new FaunusEvaluationJob(script, jobId, context.getRexsterApplicationGraph(),
                convertToMap(config));

        logger.info(String.format("Faunus script [%s] assigned job id: %s", script, jobId));

        jobs.put(jobId.toString(), job);

        new Thread(new Runnable() {
            @Override
            public void run() {
                job.performJob();
            }
        }).start();

        return ExtensionResponse.ok(new HashMap<String,Object>() {{ put(FIELD_JOB, jobId.toString()); }});
    }

    @ExtensionDefinition(extensionPoint = ExtensionPoint.GRAPH, method = HttpMethod.GET)
    @ExtensionDescriptor(description = "Get Faunus job status")
    public ExtensionResponse evaluateGet(@RexsterContext final RexsterResourceContext context) {
        final String job = context.getRequestObject().optString("job");
        if (job == null || job.isEmpty()) {
            ExtensionMethod extMethod = context.getExtensionMethod();
            return ExtensionResponse.error("the job parameter cannot be empty", null,
                    Response.Status.BAD_REQUEST.getStatusCode(), null, generateErrorJson(extMethod.getExtensionApiAsJson()));
        }

        if (!jobs.containsKey(job)) {
            return new ExtensionResponse(Response.status(Response.Status.NOT_FOUND).build());
        }

        synchronized (this) {
            final FaunusEvaluationJob fej = jobs.get(job);
            final String status = fej.isComplete() ? (fej.isError() ? STATUS_ERROR : STATUS_COMPLETE) : STATUS_PROCESSING;

            if (fej.isComplete()) {
                jobs.remove(job);
            }

            return ExtensionResponse.ok(new HashMap<String,Object>(){{
                put(FIELD_JOB, job);
                put(FIELD_STATUS, status);
                put(FIELD_MESSAGE, fej.getErrorMessage());
            }});
        }
    }

    private static Map<String,String> convertToMap(final JSONObject config) {
        final Map<String,String> m = new HashMap<String, String>();

        if (config != null) {
            final Iterator itty = config.keys();
            while (itty.hasNext()) {
                final String currentKey = itty.next().toString();
                m.put(currentKey, config.optString(currentKey));
            }
        }

        return m;
    }
}
