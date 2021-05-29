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

package org.janusgraph.core.schema;

import org.janusgraph.core.Cardinality;
import org.janusgraph.core.EdgeLabel;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.VertexLabel;
import org.janusgraph.core.attribute.Geoshape;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.UUID;

/**
 * {@link org.janusgraph.core.schema.DefaultSchemaMaker} implementation for Blueprints graphs
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class JanusGraphDefaultSchemaMaker implements DefaultSchemaMaker {

    public static final DefaultSchemaMaker INSTANCE = new JanusGraphDefaultSchemaMaker();
    private static final Logger log = LoggerFactory.getLogger(JanusGraphDefaultSchemaMaker.class);

    private boolean loggingEnabled;

    private JanusGraphDefaultSchemaMaker() {
    }

    @Override
    public void enableLogging(Boolean enabled) {
        if (Boolean.TRUE.equals(enabled)) {
            loggingEnabled = true;
        }
    }

    @Override
    public Cardinality defaultPropertyCardinality(String key) {
        return Cardinality.SINGLE;
    }

    @Override
    public EdgeLabel makeEdgeLabel(EdgeLabelMaker factory) {
        logWarn("Edge label '{}' does not exist, will create now", factory.getName());
        return DefaultSchemaMaker.super.makeEdgeLabel(factory);
    }

    @Override
    public PropertyKey makePropertyKey(PropertyKeyMaker factory) {
        logWarn("Property key '{}' does not exist, will create now", factory.getName());
        return DefaultSchemaMaker.super.makePropertyKey(factory);
    }

    @Override
    public PropertyKey makePropertyKey(PropertyKeyMaker factory, Object value) {
        String name = factory.getName();
        logWarn("Property key '{}' does not exist, will create now", name);
        Class actualClass = determineClass(value);
        if (factory.cardinalityIsSet()) {
            return factory.dataType(actualClass).make();
        }
        return factory.cardinality(defaultPropertyCardinality(name)).dataType(actualClass).make();
    }

    @Override
    public VertexLabel makeVertexLabel(VertexLabelMaker factory) {
        logWarn("Vertex label '{}' does not exist, will create now", factory.getName());
        return DefaultSchemaMaker.super.makeVertexLabel(factory);
    }

    @Override
    public void makePropertyConstraintForVertex(VertexLabel vertexLabel, PropertyKey key,
                                                SchemaManager manager) {
        logWarn("Property key constraint does not exist for given vertex label '{}' and property key '{}', will " +
                "create now", vertexLabel, key);
        DefaultSchemaMaker.super.makePropertyConstraintForVertex(vertexLabel, key, manager);
    }

    @Override
    public void makePropertyConstraintForEdge(EdgeLabel edgeLabel, PropertyKey key,
                                              SchemaManager manager) {
        logWarn("Property key constraint does not exist for given edge label '{}' and property key '{}', will " +
                "create now", edgeLabel, key);
        DefaultSchemaMaker.super.makePropertyConstraintForEdge(edgeLabel, key, manager);
    }

    @Override
    public void makeConnectionConstraint(EdgeLabel edgeLabel, VertexLabel outVLabel,
                                         VertexLabel inVLabel, SchemaManager manager) {
        logWarn("Connection constraint does not exist for given edge label '{}', outgoing vertex label '{}' and " +
                "incoming vertex label '{}', will create now", edgeLabel, outVLabel, inVLabel);
        DefaultSchemaMaker.super.makeConnectionConstraint(edgeLabel, outVLabel, inVLabel, manager);
    }

    @Override
    public boolean ignoreUndefinedQueryTypes() {
        return true;
    }

    private void logWarn(String message, Object... objects) {
        if (loggingEnabled) {
            log.warn(message, objects);
        }
    }

    protected Class determineClass(Object value) {
        if (value instanceof String) {
            return String.class;
        } else if (value instanceof Character) {
            return Character.class;
        } else if (value instanceof Boolean) {
            return Boolean.class;
        } else if (value instanceof Byte) {
            return Byte.class;
        } else if (value instanceof Short) {
            return Short.class;
        } else if (value instanceof Integer) {
            return Integer.class;
        } else if (value instanceof Long) {
            return Long.class;
        } else if (value instanceof Float) {
            return Float.class;
        } else if (value instanceof Double) {
            return Double.class;
        } else if (value instanceof Date) {
            return Date.class;
        } else if (value instanceof Geoshape) {
            return Geoshape.class;
        } else if (value instanceof UUID) {
            return UUID.class;
        } else {
            return Object.class;
        }
    }
}
