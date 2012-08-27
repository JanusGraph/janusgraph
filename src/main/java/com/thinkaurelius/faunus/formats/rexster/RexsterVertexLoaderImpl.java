package com.thinkaurelius.faunus.formats.rexster;

import org.apache.commons.io.IOUtils;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.codehaus.jettison.json.JSONTokener;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.StringWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class RexsterVertexLoaderImpl implements RexsterVertexLoader {

    private final RexsterConfiguration rexsterConf;
    private static final String script = readScript();

    public RexsterVertexLoaderImpl(final RexsterConfiguration rexsterConf) {
        this.rexsterConf = rexsterConf;
    }

    @Override
    public JSONArray getPagedVertices(long start, long end) {
        try {
            final Map<String, Object> gremlinParams = new HashMap<String, Object>();
            gremlinParams.put("start", start);
            gremlinParams.put("end", end);
            final JSONObject jsonGremlinParameters = new JSONObject(gremlinParams);

            final JSONObject jsonGremlinScript = new JSONObject();
            jsonGremlinScript.put("params", jsonGremlinParameters);
            jsonGremlinScript.put("script", script);

            return postResultObject(this.rexsterConf.getRestEndpoint(), jsonGremlinScript);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static String readScript() {
        try {
            final InputStream inputStream = RexsterVertexLoaderImpl.class.getResourceAsStream("faunus-gremlin.txt");
            final StringWriter writer = new StringWriter();
            IOUtils.copy(inputStream, writer, "UTF-8");
            return writer.toString();
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    public static JSONArray postResultObject(final String uri, final JSONObject jsonToPost) {
        try {
            // convert querystring into POST form data
            URL url = new URL(uri);
            final HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestProperty("Accept", "application/json");
            connection.setRequestProperty("Content-Type", "application/json");
            connection.setRequestMethod("POST");

            // worry about rexster auth later
            // connection.setRequestProperty(RexsterTokens.AUTHORIZATION, Authentication.getAuthenticationHeaderValue());

            connection.setDoOutput(true);
            final OutputStreamWriter writer = new OutputStreamWriter(connection.getOutputStream());
            writer.write(jsonToPost.toString());
            writer.close();

            final JSONObject json = new JSONObject(new JSONTokener(convertStreamToString(connection.getInputStream())));
            return json.optJSONArray("results");
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    private static String convertStreamToString(final InputStream is) throws Exception {
        final BufferedReader reader = new BufferedReader(new InputStreamReader(is));
        final StringBuilder sb = new StringBuilder();

        String line;
        while ((line = reader.readLine()) != null) {
            sb.append(line + "\n");
        }
        is.close();

        return sb.toString();
    }
}
