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

package org.janusgraph.graphdb.tinkerpop;

import org.janusgraph.core.attribute.Geoshape;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.DefaultSchemaMaker;
import org.janusgraph.core.schema.PropertyKeyMaker;

import java.util.Date;
import java.util.UUID;

/**
 * {@link org.janusgraph.core.schema.DefaultSchemaMaker} implementation for Blueprints graphs
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class JanusGraphDefaultSchemaMaker implements DefaultSchemaMaker {

    public static final DefaultSchemaMaker INSTANCE = new JanusGraphDefaultSchemaMaker();

    private JanusGraphDefaultSchemaMaker() {
    }

    @Override
    public Cardinality defaultPropertyCardinality(String key) {
        return Cardinality.SINGLE;
    }

    @Override
    public PropertyKey makePropertyKey(PropertyKeyMaker factory, Object value) {
        String name = factory.getName();
        Class actualClass = determineClass(value);
        if (factory.cardinalityIsSet()) {
            return factory.dataType(actualClass).make();
        }
        return factory.cardinality(defaultPropertyCardinality(name)).dataType(actualClass).make();
    }

    @Override
    public boolean ignoreUndefinedQueryTypes() {
        return true;
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
