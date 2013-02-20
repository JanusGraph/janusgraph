package com.thinkaurelius.titan.tinkerpop.rexster;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.TitanException;
import com.tinkerpop.rexster.client.RexProException;
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
import java.lang.reflect.ParameterizedType;
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
            client = RexsterClientFactory.open(host, port);
        } catch (Exception e) {
            throw new RuntimeException("Could not initialize RexsterClient", e);
        }
    }

    public RexsterTitanClient(final Configuration clientConfig) {
        Preconditions.checkNotNull(clientConfig);
        try {
            client = RexsterClientFactory.open(clientConfig);
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
        return "g=rexster.getGraph('" + RexsterTitanServer.DEFAULT_GRAPH_NAME + "'); " + query;
    }

    public List<Map<String, Object>> query(String query) {
        return query(query, null);
    }

    public List<Map<String, Object>> query(String query, Map<String, Object> parameters) {
        try {
            return client.execute(wrapQuery(query),parameters);
        } catch (MessageTypeException e) {
            throw new IllegalArgumentException("Could not convert query result", e);
        } catch (RexProException e) {
            throw new TitanException("Could not process query",e);
        } catch (IOException e) {
            throw new TitanException("Could not connect to query server",e);
        }
    }

    private static final Object convert(Value v) {
        if (v.isNilValue()) return null;
        else if (v.isBooleanValue()) return v.asBooleanValue().getBoolean();
        else if (v.isIntegerValue()) return v.asIntegerValue().getLong();
        else if (v.isFloatValue()) return v.asFloatValue().getDouble();
        else if (v.isRawValue()) return v.asRawValue().getString();
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
