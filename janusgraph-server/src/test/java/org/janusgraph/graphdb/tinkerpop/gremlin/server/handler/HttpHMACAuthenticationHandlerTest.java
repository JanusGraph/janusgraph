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

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.tinkerpop.gremlin.server.auth.AuthenticatedUser;
import org.apache.tinkerpop.gremlin.server.auth.Authenticator;
import org.easymock.Capture;
import org.easymock.CaptureType;
import org.easymock.EasyMock;
import org.easymock.EasyMockSupport;
import org.easymock.IArgumentMatcher;
import org.junit.jupiter.api.Test;

import java.util.Base64;
import java.util.Map;

import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpResponseStatus.UNAUTHORIZED;
import static org.easymock.EasyMock.and;
import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.isA;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class HttpHMACAuthenticationHandlerTest extends EasyMockSupport {

    @Test
    public void testChannelReadBasicAuthNoAuthHeader() {
        final ChannelHandlerContext ctx = createMock(ChannelHandlerContext.class);
        final FullHttpRequest msg = createMock(FullHttpRequest.class);
        final HttpHeaders headers = createMock(HttpHeaders.class);
        final Authenticator authenticator = createMock(Authenticator.class);
        final ChannelFuture cf = createMock(ChannelFuture.class);

        expect(msg.getMethod()).andReturn(HttpMethod.POST);
        expect(msg.headers()).andReturn(headers).anyTimes();
        expect(headers.get(eq("Authorization"))).andReturn(null);
        expect(ctx.writeAndFlush(eqHttpStatus(UNAUTHORIZED))).andReturn(cf);
        expect(cf.addListener(ChannelFutureListener.CLOSE)).andReturn(null);
        expect(msg.release()).andReturn(false);
        HttpHMACAuthenticationHandler handler = new HttpHMACAuthenticationHandler(authenticator);
        replayAll();
        handler.channelRead(ctx, msg);
        verifyAll();
    }

    @Test
    public void testChannelReadBasicAuthIncorrectScheme() {
        final ChannelHandlerContext ctx = createMock(ChannelHandlerContext.class);
        final FullHttpRequest msg = createMock(FullHttpRequest.class);
        final HttpHeaders headers = createMock(HttpHeaders.class);
        final Authenticator authenticator = createMock(Authenticator.class);
        final ChannelFuture cf = createMock(ChannelFuture.class);

        expect(msg.getMethod()).andReturn(HttpMethod.POST);
        expect(msg.headers()).andReturn(headers).anyTimes();
        expect(headers.get("Authorization")).andReturn("bogus");
        expect(ctx.writeAndFlush(eqHttpStatus(UNAUTHORIZED))).andReturn(cf);
        expect(cf.addListener(ChannelFutureListener.CLOSE)).andReturn(null);
        expect(msg.release()).andReturn(false);

        final HttpHMACAuthenticationHandler handler = new HttpHMACAuthenticationHandler(authenticator);
        replayAll();
        handler.channelRead(ctx, msg);
        verifyAll();
    }

    @Test
    public void testChannelReadBasicAuth() throws Exception {
        final ChannelHandlerContext ctx = createMock(ChannelHandlerContext.class);
        final FullHttpRequest msg = createMock(FullHttpRequest.class);
        final HttpHeaders headers = createMock(HttpHeaders.class);
        final Authenticator authenticator = createMock(Authenticator.class);
        final String encodedUserNameAndPass = Base64.getEncoder().encodeToString("user:pass".getBytes());
        expect(msg.getMethod()).andReturn(HttpMethod.POST);
        expect(msg.headers()).andReturn(headers).anyTimes();
        expect(msg.getUri()).andReturn("/");
        expect(headers.get(eq("Authorization"))).andReturn("Basic " + encodedUserNameAndPass);
        expect(ctx.fireChannelRead(isA(FullHttpRequest.class))).andReturn(ctx);
        expect(authenticator.authenticate(isA(Map.class))).andReturn(new AuthenticatedUser("foo"));
        final HttpHMACAuthenticationHandler handler = new HttpHMACAuthenticationHandler(authenticator);
        replayAll();
        handler.channelRead(ctx, msg);
        verifyAll();
    }

    @Test
    public void testChannelReadGetAuthToken() throws Exception {
        final ChannelHandlerContext ctx = createMock(ChannelHandlerContext.class);
        final FullHttpRequest msg = createMock(FullHttpRequest.class);
        final HttpHeaders headers = createMock(HttpHeaders.class);
        final Authenticator authenticator = createMock(Authenticator.class);
        final ChannelFuture cf = createMock(ChannelFuture.class);
        final String encodedUserNameAndPass = Base64.getEncoder().encodeToString("user:pass".getBytes());
        final Capture<Map<String, String>> credMap = EasyMock.newCapture(CaptureType.ALL);
        expect(msg.getMethod()).andReturn(HttpMethod.GET);
        expect(msg.headers()).andReturn(headers).anyTimes();
        expect(msg.getUri()).andReturn("/session");
        expect(headers.get(eq("Authorization"))).andReturn("Basic " + encodedUserNameAndPass);
        expect(authenticator.authenticate(and(isA(Map.class), capture(credMap)))).andReturn(new AuthenticatedUser("foo"));
        expect(ctx.writeAndFlush(eqHttpStatus(OK))).andReturn(cf);
        expect(cf.addListener(ChannelFutureListener.CLOSE)).andReturn(null);
        expect(msg.release()).andReturn(false);
        final HttpHMACAuthenticationHandler handler = new HttpHMACAuthenticationHandler(authenticator);
        replayAll();
        handler.channelRead(ctx, msg);
        verifyAll();
        assertNotNull(credMap.getValue().get(HttpHMACAuthenticationHandler.PROPERTY_GENERATE_TOKEN));
    }

    @Test
    public void testChannelReadTokenAuth() throws Exception {
        final ChannelHandlerContext ctx = createMock(ChannelHandlerContext.class);
        final FullHttpRequest msg = createMock(FullHttpRequest.class);
        final HttpHeaders headers = createMock(HttpHeaders.class);
        final Authenticator authenticator = createMock(Authenticator.class);
        final String encodedToken = Base64.getEncoder().encodeToString("askdjhf823asdlkfsasd".getBytes());
        expect(msg.getMethod()).andReturn(HttpMethod.GET);
        expect(msg.headers()).andReturn(headers).anyTimes();
        expect(msg.getUri()).andReturn("/");
        expect(headers.get(eq("Authorization"))).andReturn("Token " + encodedToken);
        expect(ctx.fireChannelRead(isA(FullHttpRequest.class))).andReturn(ctx);
        expect(authenticator.authenticate(isA(Map.class))).andReturn(new AuthenticatedUser("foo"));
        final HttpHMACAuthenticationHandler handler = new HttpHMACAuthenticationHandler(authenticator);
        replayAll();
        handler.channelRead(ctx, msg);
        verifyAll();
    }


    private static DefaultFullHttpResponse eqHttpStatus(HttpResponseStatus status) {
        EasyMock.reportMatcher(new WithCorrectHttpResponse(status));
        return null;
    }

    static class WithCorrectHttpResponse implements IArgumentMatcher {
        private HttpResponseStatus expected;

        public WithCorrectHttpResponse(HttpResponseStatus expected) {
            this.expected = expected;
        }

        public boolean matches(Object actual) {
            if (!(actual instanceof DefaultFullHttpResponse)) {
                return false;
            }
            return expected.equals(((DefaultFullHttpResponse) actual).getStatus());
        }

        public void appendTo(StringBuffer buffer) {
            buffer.append("eqHttpStatus(");
            buffer.append(expected.getClass().getName());
            buffer.append(" with status \"");
            buffer.append(expected.toString());
            buffer.append("\")");
        }
    }

}
