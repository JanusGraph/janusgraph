package com.thinkaurelius.titan.tinkerpop.rexster;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.TitanException;
import com.tinkerpop.rexster.client.RexsterClient;
import com.tinkerpop.rexster.client.RexsterClientFactory;
import com.tinkerpop.rexster.server.RexsterSettings;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;
import org.msgpack.MessageTypeException;
import org.msgpack.template.Template;
import org.msgpack.type.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.msgpack.template.Templates.*;

/**
 * Basic client for interacting with a Titan backed Rexster Server
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class RexsterTitanClient {

    private static final Logger log = LoggerFactory.getLogger(RexsterTitanClient.class);
    private static final int DEFAULT_TIMEOUT_MS = 3000;

    private final RexsterClient client;

    public RexsterTitanClient(String host) {
        this(host, RexsterSettings.DEFAULT_REXPRO_PORT);
    }

    public RexsterTitanClient(String host, int port) {
        Preconditions.checkArgument(!StringUtils.isEmpty(host));
        try {
            client = RexsterClientFactory.getInstance().createClient(host, port);
        } catch (Exception e) {
            throw new RuntimeException("Could not initialize RexsterClient", e);
        }
    }

    public RexsterTitanClient(final Configuration clientConfig) {
        Preconditions.checkNotNull(clientConfig);
        try {
            client = RexsterClientFactory.getInstance().createClient(clientConfig);
        } catch (Exception e) {
            throw new RuntimeException("Could not initialize RexsterClient", e);
        }
    }

    public void close() {
        try {
            client.close();
        } catch (IOException e) {
            throw new TitanException("Could not close RexsterClient", e);
        }
    }

    private static final String wrapQuery(String query) {
        return "g=rexster.getGraph('" + RexsterTitanServer.DEFAULT_GRAPH_NAME + "'); g.stopTransaction(SUCCESS); try { " + query + " } catch (Throwable e) { g.stopTransaction(FAILURE); throw e }";
    }

    public List<Map<String, Object>> query(String query) {
        return query(query, null);
    }

    public List<Map<String, Object>> query(String query, Map<String, Object> parameters) {
        List<Map<String, Value>> packResults = queryTemplate(query, parameters, tMap(TString, TValue));


        final List<Map<String, Object>> results = new ArrayList<Map<String, Object>>();
        try {
            for (Map<String, Value> map : packResults) {
                //Convert map
                Map<String, Object> result = new HashMap<String, Object>();
                for (Map.Entry<String, Value> entry : map.entrySet()) {
                    result.put(entry.getKey(), convert(entry.getValue()));
                }
                results.add(result);
            }
        } catch (MessageTypeException e) {
            throw new IllegalArgumentException("Could not convert query result", e);
        }

        return results;
    }

    public <T> List<T> queryTemplate(final String script, final Template template) {
        return queryTemplate(script, null, template);
    }

    public <T> List<T> queryTemplate(String query, final Map<String, Object> parameters, final Template template) {
        query = wrapQuery(query);
        try {
            log.trace("Sending query: {}", query);
            return client.execute(query, parameters, template);
        } catch (Throwable e) {
            log.debug("Failure in sending query", e);
            throw new TitanException("Failure in query sending", e);
        }
    }

    private static final Object convert(Value v) {
        if (v.isNilValue()) return null;
        else if (v.isBooleanValue()) return v.asBooleanValue().getBoolean();
        else if (v.isIntegerValue()) return v.asIntegerValue().getLong();
        else if (v.isFloatValue()) return v.asFloatValue().getDouble();
        else if (v.isArrayValue()) {
            Value[] arr = v.asArrayValue().getElementArray();
            Object[] result = new Object[arr.length];
            for (int i = 0; i < result.length; i++) result[i] = convert(arr[i]);
            return result;
        } else if (v.isMapValue()) {
            Map<Object, Object> result = new HashMap<Object, Object>();
            for (Map.Entry<Value, Value> entry : v.asMapValue().entrySet()) {
                result.put(convert(entry.getKey()), convert(entry.getValue()));
            }
            return result;
        } else {
            log.trace("Cannot convert value:  {} [{}]", v, v.getType());
            return v.toString();
        }
    }

}
