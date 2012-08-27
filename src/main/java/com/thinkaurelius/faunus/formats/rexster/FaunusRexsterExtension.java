package com.thinkaurelius.faunus.formats.rexster;

import com.thinkaurelius.faunus.formats.graphson.GraphSONUtility;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.rexster.RexsterResourceContext;
import com.tinkerpop.rexster.extension.AbstractRexsterExtension;
import com.tinkerpop.rexster.extension.ExtensionDefinition;
import com.tinkerpop.rexster.extension.ExtensionDescriptor;
import com.tinkerpop.rexster.extension.ExtensionNaming;
import com.tinkerpop.rexster.extension.ExtensionPoint;
import com.tinkerpop.rexster.extension.ExtensionResponse;
import com.tinkerpop.rexster.extension.RexsterContext;
import com.tinkerpop.rexster.util.RequestObjectHelper;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Streams the vertex list back.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@ExtensionNaming(namespace = FaunusRexsterExtension.EXTENSION_NAMESPACE, name = FaunusRexsterExtension.EXTENSION_NAME)
public class FaunusRexsterExtension extends AbstractRexsterExtension {

    public static final String EXTENSION_NAMESPACE = "aurelius";
    public static final String EXTENSION_NAME = "v";

    public static final byte[] LINE_BREAK = "\n".getBytes();

    @ExtensionDefinition(extensionPoint = ExtensionPoint.GRAPH, produces = MediaType.APPLICATION_OCTET_STREAM)
    @ExtensionDescriptor(description = "streaming vertices for faunus")
    public ExtensionResponse getVertices(@RexsterContext final RexsterResourceContext context,
                                         @RexsterContext final Graph graph) {
        final long start = RequestObjectHelper.getStartOffset(context.getRequestObject());
        final long end = RequestObjectHelper.getEndOffset(context.getRequestObject());

        return new ExtensionResponse(Response.ok(new StreamingOutput() {
            @Override
            public void write(OutputStream out) throws IOException {
                long counter = 0;

                final Iterable<Vertex> vertices = graph.getVertices();
                for (Vertex vertex : vertices) {
                    if (counter >= start && counter < end) {
                        final byte [] jsonBytes = GraphSONUtility.toJSON(vertex).toString().getBytes();
                        out.write(jsonBytes);
                        out.write(LINE_BREAK);
                    } else if (counter >= end) {
                        break;
                    }
                    counter++;
                }
            }
        }).build());
    }

}
