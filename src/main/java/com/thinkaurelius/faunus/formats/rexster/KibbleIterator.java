package com.thinkaurelius.faunus.formats.rexster;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

/**
 * KibbleIterator uses the FaunusRexsterExtension Kibble to collect vertices from Rexster.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class KibbleIterator implements RexsterIterator {

    private final RexsterConfiguration rexsterConf;
    private final long start;
    private final long end;
    private long itemsIterated = 0;

    private BufferedReader streamReader;
    private String currentJsonLine;

    public KibbleIterator(final RexsterConfiguration rexsterConf, final long start, final long end) {
        this.rexsterConf = rexsterConf;
        this.start = start;
        this.end = end;

        this.openStream();
        this.advance();
    }

    @Override
    public boolean hasNext() {
        return this.currentJsonLine != null;
    }

    @Override
    public JSONObject next() {
        try {
            final JSONObject nextLine = new JSONObject(currentJsonLine);
            advance();

            this.itemsIterated++;

            return nextLine;
        } catch (JSONException jsone) {
            throw new RuntimeException(jsone);
        }
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getItemsIterated() {
        return this.itemsIterated;
    }

    private void advance() {
        try {
            currentJsonLine = streamReader.readLine();
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    private void openStream() {
        try {
            final String uri = String.format("%s?rexster.offset.start=%s&rexster.offset.end=%s",
                    this.rexsterConf.getRestStreamEndpoint(), this.start, this.end);
            final URL url = new URL(uri);
            final HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setConnectTimeout(0);
            connection.setReadTimeout(0);
            connection.setRequestMethod("GET");

            // worry about rexster auth later
            // connection.setRequestProperty(RexsterTokens.AUTHORIZATION, Authentication.getAuthenticationHeaderValue());

            connection.setDoOutput(true);

            this.streamReader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }
}
