package com.thinkaurelius.faunus.formats.rexster.util;

import java.net.HttpURLConnection;
import java.net.URL;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class HttpHelper {

    public static HttpURLConnection createConnection(final String uri, final String authValue) throws Exception {
        final URL url = new URL(uri);
        final HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setConnectTimeout(0);
        connection.setReadTimeout(0);
        connection.setRequestMethod("GET");

        connection.setRequestProperty("Authorization", authValue);

        connection.setDoOutput(true);

        return connection;
    }
}
