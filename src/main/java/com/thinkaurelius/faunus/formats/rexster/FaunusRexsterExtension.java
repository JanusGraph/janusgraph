package com.thinkaurelius.faunus.formats.rexster;

import com.thinkaurelius.faunus.formats.rexster.util.DefaultElementIdHandler;
import com.thinkaurelius.faunus.formats.rexster.util.ElementIdHandler;
import com.thinkaurelius.faunus.formats.rexster.util.OrientElementIdHandler;
import com.thinkaurelius.faunus.formats.rexster.util.TitanBerkeleyJEElementIdHandler;
import com.thinkaurelius.faunus.formats.rexster.util.VertexToFaunusBinary;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.rexster.RexsterApplicationGraph;
import com.tinkerpop.rexster.RexsterResourceContext;
import com.tinkerpop.rexster.extension.AbstractRexsterExtension;
import com.tinkerpop.rexster.extension.ExtensionConfiguration;
import com.tinkerpop.rexster.extension.ExtensionDefinition;
import com.tinkerpop.rexster.extension.ExtensionDescriptor;
import com.tinkerpop.rexster.extension.ExtensionNaming;
import com.tinkerpop.rexster.extension.ExtensionPoint;
import com.tinkerpop.rexster.extension.ExtensionResponse;
import com.tinkerpop.rexster.extension.HttpMethod;
import com.tinkerpop.rexster.extension.RexsterContext;
import com.tinkerpop.rexster.util.RequestObjectHelper;
import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONObject;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Streams the vertex list back in FaunusVertex binary format.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@ExtensionNaming(namespace = FaunusRexsterExtension.EXTENSION_NAMESPACE, name = FaunusRexsterExtension.EXTENSION_NAME)
public class FaunusRexsterExtension extends AbstractRexsterExtension {
    private static final Logger logger = Logger.getLogger(FaunusRexsterExtension.class);
    private static final ElementIdHandler DEFAULT_ID_HANDLER = new DefaultElementIdHandler();

    private static final long WRITE_STATUS_EVERY = 10000;
    private static final String CONFIG_ID_HANDLER = "id-handler";

    public static final String EXTENSION_NAMESPACE = "faunus";
    public static final String EXTENSION_NAME = "rexsterinputformat";
    public static final String EXTENSION_METHOD_STREAM = "stream";
    public static final String EXTENSION_METHOD_COUNT = "count";

    @ExtensionDefinition(extensionPoint = ExtensionPoint.GRAPH, produces = MediaType.APPLICATION_JSON,
            method = HttpMethod.GET, path = EXTENSION_METHOD_COUNT)
    @ExtensionDescriptor(description = "get true count of vertices to calculate true split size for faunus")
    public ExtensionResponse getVertexCount(@RexsterContext final Graph graph) {
        logger.info("Faunus is configured to get the true count of vertices in the graph.");

        int counter = 0;
        final Iterable<Vertex> vertices = graph.getVertices();
        for (Vertex v : vertices) {
            counter++;

            if (logger.isDebugEnabled() && counter % WRITE_STATUS_EVERY == 0) {
                logger.debug(String.format("True count at: %s", counter));
            }
        }

        final Map<String, Integer> m = new HashMap<String, Integer>();
        m.put(EXTENSION_METHOD_COUNT, counter);

        return ExtensionResponse.ok(m);
    }

    @ExtensionDefinition(extensionPoint = ExtensionPoint.GRAPH, produces = MediaType.APPLICATION_OCTET_STREAM,
            method = HttpMethod.GET, path = EXTENSION_METHOD_STREAM)
    @ExtensionDescriptor(description = "streaming vertices for faunus")
    public ExtensionResponse getVertices(@RexsterContext final RexsterResourceContext context,
                                         @RexsterContext final Graph graph) {
        final JSONObject requestObject = context.getRequestObject();
        final long start = RequestObjectHelper.getStartOffset(requestObject);
        final long end = RequestObjectHelper.getEndOffset(requestObject);

        final ElementIdHandler elementIdHandler = this.getElementIdHandler(context.getRexsterApplicationGraph());
        final VertexToFaunusBinary vertexToFaunusBinary = new VertexToFaunusBinary(elementIdHandler);

        // help with uniquely identifying incoming requests in logs.
        final UUID requestIdentifier = UUID.randomUUID();
        final String verticesInSplit = end == Long.MAX_VALUE ? "END" : String.valueOf(end - start);
        logger.debug(String.format("Request [%s] split between [%s] and [%s].", requestIdentifier, start, end));

        return new ExtensionResponse(Response.ok(new StreamingOutput() {
            @Override
            public void write(OutputStream out) throws IOException {
                long counter = 0;
                long vertexCount = 0;

                final DataOutputStream dos = new DataOutputStream(out);

                final Iterable<Vertex> vertices = graph.getVertices();
                for (Vertex vertex : vertices) {
                    if (counter >= start && counter < end) {
                        vertexToFaunusBinary.writeVertex(vertex, dos);

                        if (logger.isDebugEnabled() && counter % WRITE_STATUS_EVERY == 0) {
                            logger.debug(String.format("Request [%s] at [%s] on the way to [%s].",
                                    requestIdentifier, vertexCount, verticesInSplit));
                        }

                        vertexCount++;

                    } else if (counter >= end) {
                        logger.debug(String.format("Request [%s] completed.", requestIdentifier));
                        break;
                    }
                    counter++;
                }
            }
        }).build());
    }

    private ElementIdHandler getElementIdHandler(final RexsterApplicationGraph rag) {
        final ExtensionConfiguration configuration = rag.findExtensionConfiguration(EXTENSION_NAMESPACE, EXTENSION_NAME);
        if (configuration == null) {
            return DEFAULT_ID_HANDLER;
        }

        final Map<String, String> map = configuration.tryGetMapFromConfiguration();
        final String idHandlerName = map.get(CONFIG_ID_HANDLER);

        if (idHandlerName.equals("orientdb"))
            return new OrientElementIdHandler();
        else if (idHandlerName.equals("titan-berkeleyje"))
            return new TitanBerkeleyJEElementIdHandler();
        else
            return DEFAULT_ID_HANDLER;
    }
}
