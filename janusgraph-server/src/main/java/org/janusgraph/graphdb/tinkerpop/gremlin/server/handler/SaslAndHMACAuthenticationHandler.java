// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.graphdb.tinkerpop.gremlin.server.handler;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.HttpMessage;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.tinkerpop.gremlin.server.auth.Authenticator;
import org.apache.tinkerpop.gremlin.server.handler.SaslAuthenticationHandler;
import org.janusgraph.graphdb.tinkerpop.gremlin.server.auth.HMACAuthenticator;
import org.janusgraph.graphdb.tinkerpop.gremlin.server.auth.SaslAndHMACAuthenticator;

import static io.netty.handler.codec.http.HttpHeaders.Names.CONNECTION;
import static io.netty.handler.codec.http.HttpHeaders.Names.UPGRADE;
import static org.apache.tinkerpop.gremlin.server.AbstractChannelizer.PIPELINE_AUTHENTICATOR;

/**
 * A class for doing Basic Auth and Token auth using an HMAC as well as
 * Sasl authentication
 * @author Keith Lohnes lohnesk@gmail.com
 */
@ChannelHandler.Sharable
public class SaslAndHMACAuthenticationHandler extends SaslAuthenticationHandler {

    private final String HMAC_AUTH = "hmac_authenticator";
    private HMACAuthenticator hmacAuthenticator;

    public SaslAndHMACAuthenticationHandler(final Authenticator authenticator, final Settings authenticationSettings) {
        super(((SaslAndHMACAuthenticator) authenticator).getSimpleAuthenticator(), authenticationSettings);
        hmacAuthenticator = ((SaslAndHMACAuthenticator) authenticator).getHMACAuthenticator();
    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, final Object obj) throws Exception {
        if (obj instanceof HttpMessage && !isWebSocket((HttpMessage)obj)) {
            if (null == ctx.pipeline().get(HMAC_AUTH)) {
                final HttpHMACAuthenticationHandler authHandler = new HttpHMACAuthenticationHandler(hmacAuthenticator);
                ctx.pipeline().addAfter(PIPELINE_AUTHENTICATOR, HMAC_AUTH, authHandler);
            }
            ctx.fireChannelRead(obj);
        } else {
            super.channelRead(ctx, obj);
        }
    }

    private boolean isWebSocket(final HttpMessage msg) {
        final String connectionHeader = msg.headers().get(CONNECTION);
        final String upgradeHeader = msg.headers().get(UPGRADE);
        return (null != connectionHeader && connectionHeader.equalsIgnoreCase("Upgrade")) ||
               (null != upgradeHeader && upgradeHeader.equalsIgnoreCase("WebSocket"));
    }

}
