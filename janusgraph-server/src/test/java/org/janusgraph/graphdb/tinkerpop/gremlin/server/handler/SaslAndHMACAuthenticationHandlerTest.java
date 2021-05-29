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
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMessage;
import org.easymock.EasyMockSupport;
import org.janusgraph.graphdb.tinkerpop.gremlin.server.auth.HMACAuthenticator;
import org.janusgraph.graphdb.tinkerpop.gremlin.server.auth.JanusGraphSimpleAuthenticator;
import org.janusgraph.graphdb.tinkerpop.gremlin.server.auth.SaslAndHMACAuthenticator;
import org.junit.jupiter.api.Test;

import static org.apache.tinkerpop.gremlin.server.AbstractChannelizer.PIPELINE_AUTHENTICATOR;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.isA;

public class SaslAndHMACAuthenticationHandlerTest extends EasyMockSupport {

    @Test
    public void testHttpChannelReadWhenAuthenticatorHasNotBeenAdded() throws Exception {
        final HMACAuthenticator hmacAuth = createMock(HMACAuthenticator.class);
        final SaslAndHMACAuthenticator authenticator = createMock(SaslAndHMACAuthenticator.class);
        final ChannelHandlerContext ctx = createMock(ChannelHandlerContext.class);
        final ChannelPipeline pipeline = createMock(ChannelPipeline.class);
        final HttpMessage msg = createMock(HttpMessage.class);
        final HttpHeaders headers = createMock(HttpHeaders.class);

        expect(authenticator.getHMACAuthenticator()).andReturn(hmacAuth);
        expect(authenticator.getSimpleAuthenticator()).andReturn(createMock(JanusGraphSimpleAuthenticator.class));
        expect(ctx.pipeline()).andReturn(pipeline).times(2);
        expect(pipeline.get("hmac_authenticator")).andReturn(null);
        expect(pipeline.addAfter(eq(PIPELINE_AUTHENTICATOR), eq("hmac_authenticator"), isA(ChannelHandler.class))).andReturn(null);
        expect(msg.headers()).andReturn(headers).times(2);
        expect(headers.get(isA(String.class))).andReturn(null).times(2);
        expect(ctx.fireChannelRead(eq(msg))).andReturn(ctx);
        replayAll();

        final SaslAndHMACAuthenticationHandler handler = new SaslAndHMACAuthenticationHandler(authenticator, null);
        handler.channelRead(ctx, msg);
    }

    @Test
    public void testHttpChannelReadWhenAuthenticatorHasBeenAdded() throws Exception {
        final SaslAndHMACAuthenticator authenticator = createMock(SaslAndHMACAuthenticator.class);
        final HMACAuthenticator hmacAuth = createMock(HMACAuthenticator.class);
        final ChannelHandlerContext ctx = createMock(ChannelHandlerContext.class);
        final ChannelHandler mockHandler = createMock(ChannelHandler.class);
        final ChannelPipeline pipeline = createMock(ChannelPipeline.class);
        final HttpMessage msg = createMock(HttpMessage.class);
        final HttpHeaders headers = createMock(HttpHeaders.class);

        expect(authenticator.getHMACAuthenticator()).andReturn(hmacAuth);
        expect(authenticator.getSimpleAuthenticator()).andReturn(createMock(JanusGraphSimpleAuthenticator.class));
        expect(ctx.pipeline()).andReturn(pipeline);
        expect(pipeline.get("hmac_authenticator")).andReturn(mockHandler);
        expect(msg.headers()).andReturn(headers).times(2);
        expect(headers.get(isA(String.class))).andReturn(null).times(2);
        expect(ctx.fireChannelRead(eq(msg))).andReturn(ctx);
        replayAll();

        final SaslAndHMACAuthenticationHandler handler = new SaslAndHMACAuthenticationHandler(authenticator, null);
        handler.channelRead(ctx, msg);
    }

}
