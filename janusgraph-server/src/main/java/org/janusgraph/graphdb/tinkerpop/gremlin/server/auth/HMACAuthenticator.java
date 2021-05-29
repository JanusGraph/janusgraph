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

package org.janusgraph.graphdb.tinkerpop.gremlin.server.auth;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.tinkerpop.gremlin.server.auth.AuthenticatedUser;
import org.apache.tinkerpop.gremlin.server.auth.AuthenticationException;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.mindrot.jbcrypt.BCrypt;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.Date;
import java.util.Map;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import static org.apache.tinkerpop.gremlin.groovy.jsr223.dsl.credential.CredentialGraphTokens.PROPERTY_PASSWORD;
import static org.apache.tinkerpop.gremlin.groovy.jsr223.dsl.credential.CredentialGraphTokens.PROPERTY_USERNAME;
import static org.janusgraph.graphdb.tinkerpop.gremlin.server.handler.HttpHMACAuthenticationHandler.PROPERTY_GENERATE_TOKEN;
import static org.janusgraph.graphdb.tinkerpop.gremlin.server.handler.HttpHMACAuthenticationHandler.PROPERTY_TOKEN;

/**
 * A class for doing Basic Auth and Token auth using an HMAC intended to be used with
 * the HMACAuthenticationHandler
 *
 * @author Keith Lohnes lohnesk@gmail.com
 */
public class HMACAuthenticator extends JanusGraphAbstractAuthenticator {

    /**
     * Hmac algorithm defaults to hmacsha256
     */
    public static final String CONFIG_HMAC_ALGO = "hmacAlgo";

    /**
     * How long an auth token should stay valid
     */
    public static final String CONFIG_TOKEN_TIMEOUT = "tokenTimeout";

    /**
     * Hmac secret config
     */
    public static final String CONFIG_HMAC_SECRET = "hmacSecret";

    private static final String AUTH_ERROR = "Username and/or password are incorrect";

    private static final String DEFAULT_HMAC_ALGO = "HmacSHA256";

    private static final char[] DEFAULT_HMAC_SECRET = "secret".toCharArray();

    private static final Long DEFAULT_HMAC_TOKEN_TIMEOUT = 3600000L;

    private char[] secret;
    private String hmacAlgo;
    private Long timeout;

    @Override
    public boolean requireAuthentication() {
        return true;
    }

    @Override
    public SaslNegotiator newSaslNegotiator(final InetAddress remoteAddress) {
        throw new RuntimeException("HMACAuthenticator does not use SASL!");
    }

    public SaslNegotiator newSaslNegotiator() {
        throw new RuntimeException("HMACAuthenticator does not use SASL!");
    }

    public void setup(final Map<String,Object> config) {
        Preconditions.checkArgument(config != null, "Credential configuration cannot be null");
        Preconditions.checkState(config.containsKey(CONFIG_HMAC_SECRET), "Credential configuration missing the %s key", CONFIG_HMAC_SECRET);

        if (config.containsKey(CONFIG_HMAC_ALGO)) {
            hmacAlgo = config.get(CONFIG_HMAC_ALGO).toString();
        } else {
            hmacAlgo = DEFAULT_HMAC_ALGO;
        }

        if (config.containsKey(CONFIG_TOKEN_TIMEOUT)) {
            timeout = ((Number) config.get(CONFIG_TOKEN_TIMEOUT)).longValue();
        } else {
            timeout = DEFAULT_HMAC_TOKEN_TIMEOUT;
        }

        super.setup(config);

        secret = config.containsKey(CONFIG_HMAC_SECRET) ? config.get(CONFIG_HMAC_SECRET).toString().toCharArray() :
            DEFAULT_HMAC_SECRET;
    }

    @Override
    public AuthenticatedUser authenticate(final Map<String, String> credentials) throws AuthenticationException {
        if (credentials.get(PROPERTY_GENERATE_TOKEN) != null) {
            credentials.put(PROPERTY_TOKEN, getToken(credentials));
            return authenticateUser(credentials);
        } else if (credentials.get(PROPERTY_TOKEN) != null) {
            if (validateToken(credentials)) {
                final Map<String, String> tokenMap = parseToken(credentials.get(PROPERTY_TOKEN));
                return new AuthenticatedUser(tokenMap.get(PROPERTY_USERNAME));
            } else {
                throw new AuthenticationException("Invalid token");
            }
        } else {
            return authenticateUser(credentials);
        }
    }

    private AuthenticatedUser authenticateUser(final Map<String, String> credentials) throws AuthenticationException {
        final Vertex v = findUser(credentials.get(PROPERTY_USERNAME));
        if (null == v || !BCrypt.checkpw(credentials.get(PROPERTY_PASSWORD), v.value(PROPERTY_PASSWORD))) {
            throw new AuthenticationException(AUTH_ERROR);
        }
        return new AuthenticatedUser(credentials.get(PROPERTY_USERNAME));
    }

    private boolean validateToken(Map<String, String> credentials) {
        final String token = credentials.get(PROPERTY_TOKEN);
        final Map<String, String> tokenMap = parseToken(token);
        final String username = tokenMap.get(PROPERTY_USERNAME);
        final String time = tokenMap.get("time");
        final String password = findUser(username).value(PROPERTY_PASSWORD);
        final String salt = getBcryptSaltFromStoredPassword(password);
        final String expected = generateToken(username, salt, time);
        final Long timeLong = Long.parseLong(time);
        final long currentTime = new Date().getTime();
        final String base64Token = new String(Base64.getUrlEncoder().encode(token.getBytes()));
        //Short circuit if the lengths aren't the same or time has expired
        if (timeLong + timeout < currentTime || expected.length() != base64Token.length()) {
            return false;
        } else {
            //Don't short circuit comparison to prevent timing attacks
            boolean isValid = true;
            for (int i = 0; i < expected.length(); i++) {
                if (base64Token.charAt(i) != expected.charAt(i)) {
                    isValid = false;
                    break;
                }
            }
            return isValid;
        }
    }

    private Map<String, String> parseToken(final String token) {
        final String[] parts = token.split(":");
        return ImmutableMap.of(PROPERTY_USERNAME, parts[0], "time", parts[1], "hmac", parts[2]);
    }

    private String generateToken(final String username, final String salt, final String time) {
        try {
            final CharBuffer secretAndSalt = CharBuffer.allocate(secret.length + salt.length() + 1);
            secretAndSalt.put(secret);
            secretAndSalt.put(":");
            secretAndSalt.put(salt);
            final String tokenPrefix = username + ":" + time + ":";
            final SecretKeySpec keySpec = new SecretKeySpec(toBytes(secretAndSalt.array()), hmacAlgo);
            final Mac hmac = Mac.getInstance(hmacAlgo);
            hmac.init(keySpec);
            hmac.update(username.getBytes());
            hmac.update(time.getBytes());
            final Base64.Encoder encoder = Base64.getUrlEncoder();
            final byte[] hmacbytes = encoder.encode(hmac.doFinal());
            final byte[] tokenbytes = tokenPrefix.getBytes();
            final byte[] token = ByteBuffer.wrap(new byte[tokenbytes.length + hmacbytes.length]).put(tokenbytes).put(hmacbytes).array();
            return new String(encoder.encode(token));
        } catch (Exception ex){
            throw new RuntimeException(ex);
        }
    }

    private String getToken(final Map<String, String> credentials) {
        final String username = credentials.get(PROPERTY_USERNAME);
        final Vertex user = findUser(username);
        final String password = user.value(PROPERTY_PASSWORD);
        final String salt = getBcryptSaltFromStoredPassword(password);
        final String time = Long.toString(new Date().getTime());
        return generateToken(username, salt, time);
    }

    //In BCrypt, the salt is the 22 chars after the 3rd $
    private String getBcryptSaltFromStoredPassword(String password) {
        int saltStart = StringUtils.ordinalIndexOf(password, "$", 3);
        return password.substring(saltStart + 1, saltStart + 23);
    }

    private byte[] toBytes(char[] chars) {
      CharBuffer charBuffer = CharBuffer.wrap(chars);
      ByteBuffer byteBuffer = StandardCharsets.UTF_8.encode(charBuffer);
      byte[] bytes = Arrays.copyOfRange(byteBuffer.array(),
              byteBuffer.position(), byteBuffer.limit());
      Arrays.fill(charBuffer.array(), '\u0000'); //Clear sensitive data from memory
      Arrays.fill(byteBuffer.array(), (byte) 0); //Clear sensitive data from memory
      return bytes;
  }
}
