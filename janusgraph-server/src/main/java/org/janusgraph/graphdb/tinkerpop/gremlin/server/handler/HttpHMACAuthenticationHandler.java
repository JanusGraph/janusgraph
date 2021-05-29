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

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.util.ReferenceCountUtil;
import org.apache.tinkerpop.gremlin.server.auth.Authenticator;
import org.apache.tinkerpop.gremlin.server.handler.AbstractAuthenticationHandler;

import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import static io.netty.handler.codec.http.HttpHeaders.Names.CONTENT_LENGTH;
import static io.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpResponseStatus.UNAUTHORIZED;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static org.apache.tinkerpop.gremlin.groovy.jsr223.dsl.credential.CredentialGraphTokens.PROPERTY_PASSWORD;
import static org.apache.tinkerpop.gremlin.groovy.jsr223.dsl.credential.CredentialGraphTokens.PROPERTY_USERNAME;

/**
 * An Authentication Handler intended to be used with the HMACAuthenticator
 * to provide Basic Auth and Token auth using an HMAC token
 *
 * @author Keith Lohnes lohnesk@gmail.com
 */
public class HttpHMACAuthenticationHandler extends AbstractAuthenticationHandler {

    private final Base64.Decoder decoder = Base64.getUrlDecoder();

    private final String basic = "Basic ";
    private final String token = "Token ";

    public static final String PROPERTY_TOKEN = "TOKEN";
    public static final String PROPERTY_GENERATE_TOKEN = "GENERATE_TOKEN";

    public HttpHMACAuthenticationHandler(final Authenticator authenticator) {
        super(authenticator);
    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, final Object msg) {
        if (msg instanceof FullHttpRequest) {
            final FullHttpRequest req = (FullHttpRequest) msg;
            final HttpMethod method = req.getMethod();
            final Map<String, String> credentialsMap = getCredentialsMap(ctx, req);
            if (credentialsMap == null) {
                sendError(ctx, msg);
                return;
            }
            if ("/session".equals(req.getUri()) && method.equals(HttpMethod.GET)) {
                try {
                    credentialsMap.put(PROPERTY_GENERATE_TOKEN, "true");
                    authenticator.authenticate(credentialsMap);
                } catch (Exception e) {
                    sendError(ctx, msg);
                    return;
                }
                replyWithToken(ctx, msg, credentialsMap.get(PROPERTY_TOKEN));
            } else {
                try {
                    authenticator.authenticate(credentialsMap);
                    ctx.fireChannelRead(req);
                } catch (Exception e) {
                    sendError(ctx, msg);
                }
            }
        }
    }

    private Map<String, String> getCredentialsMap(final ChannelHandlerContext ctx, final FullHttpRequest req) {
        // strip off "Basic " from the Authorization header (RFC 2617)
        // or "Token "
        final String authorizationHeader = req.headers().get("Authorization");
        if (authorizationHeader == null) {
            return null;
        }
        if (!(authorizationHeader.startsWith(basic) || authorizationHeader.startsWith(token))) {
            return null;
        }
        final String authType = authorizationHeader.startsWith(basic) ? basic : token;
        return getAuthCredMap(authorizationHeader, authType);
    }

    private Map<String, String> getAuthCredMap(final String authorizationHeader, final String authType) {
        final byte[] decodedAuthParams;
        final String authorization;
        try {
            final String encodedAuthParams = authorizationHeader.substring(authType.length());
            decodedAuthParams = decoder.decode(encodedAuthParams);
            authorization = new String(decodedAuthParams);
        } catch (IndexOutOfBoundsException | IllegalArgumentException e) {
            return null;
        }
        final Map<String,String> credentials = new HashMap<>();

        if (authType.equals(basic)) {
            final String[] split = authorization.split(":");
            if (split.length != 2) {
                return null;
            }
            credentials.put(PROPERTY_USERNAME, split[0]);
            credentials.put(PROPERTY_PASSWORD, split[1]);
        } else {
            credentials.put(PROPERTY_TOKEN, authorization);
        }
        return credentials;
    }

    private void sendError(final ChannelHandlerContext ctx, final Object msg) {
        ctx.writeAndFlush(new DefaultFullHttpResponse(HTTP_1_1, UNAUTHORIZED)).addListener(ChannelFutureListener.CLOSE);
        ReferenceCountUtil.release(msg);
    }

    private void replyWithToken(final ChannelHandlerContext ctx, final Object msg, final String token) {
        final String json = "{\"token\": \"" + token + "\"}";
        final byte[] jsonBytes = json.getBytes();
        final FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, OK, Unpooled.wrappedBuffer(jsonBytes));
        response.headers().set(CONTENT_TYPE, "application/json");
        response.headers().set(CONTENT_LENGTH, response.content().readableBytes());
        ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
        ReferenceCountUtil.release(msg);
    }
}
