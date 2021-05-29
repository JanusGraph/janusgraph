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

package org.janusgraph.diskstorage.configuration;

import com.google.common.base.Preconditions;
import org.janusgraph.core.schema.JanusGraphConfiguration;

import java.lang.reflect.Array;
import java.time.Duration;

/**
 * Helper class for inspecting and modifying a configuration for JanusGraph.
 * It is important to {@link #close()} the configuration when all changes have been made.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class UserModifiableConfiguration implements JanusGraphConfiguration {

    private final ModifiableConfiguration config;
    private final ConfigVerifier verifier;

    public UserModifiableConfiguration(ModifiableConfiguration config) {
        this(config,ALLOW_ALL);
    }

    public UserModifiableConfiguration(ModifiableConfiguration config, ConfigVerifier verifier) {
        Preconditions.checkArgument(config!=null && verifier!=null);
        this.config = config;
        this.verifier = verifier;
    }


    /**
     * Returns the backing configuration as a {@link ReadConfiguration} that can be used
     * to create and configure a JanusGraph graph.
     *
     * @return
     */
    public ReadConfiguration getConfiguration() {
        return config.getConfiguration();
    }

    @Override
    public String get(String path) {
        ConfigElement.PathIdentifier pp = ConfigElement.parse(config.getRootNamespace(),path);
        if (pp.element.isNamespace()) {
            ConfigNamespace ns = (ConfigNamespace)pp.element;
            StringBuilder s = new StringBuilder();
            if (ns.isUmbrella() && !pp.lastIsUmbrella) {
                for (String sub : config.getContainedNamespaces(ns,pp.umbrellaElements)) {
                    s.append("+ ").append(sub).append("\n");
                }
            } /* else {
                for (ConfigElement element : ns.getChildren()) {
                    s.append(ConfigElement.toStringSingle(element)).append("\n");
                }
            } */
            return s.toString();
        } else {
            Object value;
            if (config.has((ConfigOption)pp.element,pp.umbrellaElements) || ((ConfigOption) pp.element).getDefaultValue()!=null) {
                value = config.get((ConfigOption)pp.element,pp.umbrellaElements);
            } else {
                return "null";
            }
            Preconditions.checkNotNull(value);
            if (value.getClass().isArray()) {
                StringBuilder s = new StringBuilder();
                s.append("[");
                for (int i=0;i<Array.getLength(value);i++) {
                    if (i>0) s.append(",");
                    s.append(Array.get(value,i));
                }
                s.append("]");
                return s.toString();
            } else return String.valueOf(value);
        }
    }


    @Override
    public UserModifiableConfiguration set(String path, Object value) {
        ConfigElement.PathIdentifier pp = ConfigElement.parse(config.getRootNamespace(),path);
        Preconditions.checkArgument(pp.element.isOption(),"Need to provide configuration option - not namespace: %s",path);
        ConfigOption option = (ConfigOption)pp.element;
        verifier.verifyModification(option);
        if (option.getDatatype().isArray()) {
            Class arrayType = option.getDatatype().getComponentType();
            Object arr;
            if (value.getClass().isArray()) {
                int size = Array.getLength(value);
                arr = Array.newInstance(arrayType,size);
                for (int i=0;i<size;i++) {
                    Array.set(arr,i,convertBasic(Array.get(value,i),arrayType));
                }
            } else {
                arr = Array.newInstance(arrayType,1);
                Array.set(arr,0,convertBasic(value,arrayType));
            }
            value = arr;
        } else {
            value = convertBasic(value,option.getDatatype());
        }
        config.set(option,value,pp.umbrellaElements);
        return this;
    }

    /**
     * Closes this configuration handler
     */
    public void close() {
        config.close();
    }

    private static Object convertBasic(Object value, Class datatype) {
        if (Number.class.isAssignableFrom(datatype)) {
            Preconditions.checkArgument(value instanceof Number,"Expected a number but got: %s",value);
            Number n = (Number)value;
            if (datatype==Long.class) {
                return n.longValue();
            } else if (datatype==Integer.class) {
                return n.intValue();
            } else if (datatype==Short.class) {
                return n.shortValue();
            } else if (datatype==Byte.class) {
                return n.byteValue();
            } else if (datatype==Float.class) {
                return n.floatValue();
            } else if (datatype==Double.class) {
                return n.doubleValue();
            } else throw new IllegalArgumentException("Unexpected number data type: " + datatype);
        } else if (datatype==Boolean.class) {
            Preconditions.checkArgument(value instanceof Boolean,"Expected boolean value: %s",value);
            return value;
        } else if (datatype==String.class) {
            Preconditions.checkArgument(value instanceof String,"Expected string value: %s",value);
            return value;
        } else if (Duration.class.isAssignableFrom(datatype)) {
            Preconditions.checkArgument(value instanceof Duration,"Expected duration value: %s",value);
            return value;
        } else if (datatype.isEnum()) {
            // Check if value is an enum instance
            for (Object e : datatype.getEnumConstants())
                if (e.equals(value))
                    return e;
            // Else toString() it and try to parse it as an enum value
            for (Object e : datatype.getEnumConstants())
                if (e.toString().equals(value.toString()))
                    return e;
            throw new IllegalArgumentException("No match for " + value + " in enum " + datatype);
        } else throw new IllegalArgumentException("Unexpected data type: " + datatype );
    }


    public interface ConfigVerifier {

        /**
         * Throws an exception if the given configuration option is not allowed to be changed.
         * Otherwise just returns.
         * @param option
         */
        void verifyModification(ConfigOption option);

    }

    public static final ConfigVerifier ALLOW_ALL = option -> {
        //Do nothing;
    };


}
