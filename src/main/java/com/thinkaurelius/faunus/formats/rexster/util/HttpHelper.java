package com.thinkaurelius.faunus.formats.rexster.util;

import java.net.HttpURLConnection;
import java.net.URL;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class HttpHelper {

    public static HttpURLConnection createConnection(final String uri) throws Exception {
        final URL url = new URL(uri);
        final HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setConnectTimeout(0);
        connection.setReadTimeout(0);
        connection.setRequestMethod("GET");

        // worry about rexster auth later
        // connection.setRequestProperty(RexsterTokens.AUTHORIZATION, Authentication.getAuthenticationHeaderValue());

        connection.setDoOutput(true);

        return connection;
    }
}
