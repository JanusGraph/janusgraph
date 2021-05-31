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

package org.janusgraph.util.system;

import com.google.common.base.Preconditions;
import org.apache.commons.configuration2.BaseConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.MapConfiguration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.builder.fluent.PropertiesBuilderParameters;
import org.apache.commons.configuration2.convert.DefaultListDelimiterHandler;
import org.apache.commons.configuration2.ex.ConfigurationException;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class ConfigurationUtil {

    private static final char CONFIGURATION_SEPARATOR = '.';
    private static final DefaultListDelimiterHandler COMMA_DELIMITER_HANDLER = new DefaultListDelimiterHandler(',');

    public static List<String> getUniquePrefixes(Configuration config) {
        final Set<String> nameSet = new HashSet<>();
        final List<String> names = new ArrayList<>();
        final Iterator<String> keyIterator = config.getKeys();
        while (keyIterator.hasNext()) {
            String key = keyIterator.next();
            int pos = key.indexOf(CONFIGURATION_SEPARATOR);
            if (pos > 0) {
                String name = key.substring(0, pos);
                if (nameSet.add(name)) {
                    names.add(name);
                }
            }
        }
        return names;
    }

    public static <T> T instantiate(String className) {
        return instantiate(className,new Object[0],new Class[0]);
    }

    public static <T> T instantiate(String className, Object[] constructorArgs, Class[] classes) {
        Preconditions.checkArgument(constructorArgs!=null && classes!=null);
        Preconditions.checkArgument(constructorArgs.length==classes.length);
        try {
            Class clazz = Class.forName(className);
            Constructor constructor = clazz.getConstructor(classes);
            return (T) constructor.newInstance(constructorArgs);
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException("Could not find implementation class: " + className, e);
        } catch (NoSuchMethodException e) {
            throw new IllegalArgumentException("Implementation class does not have required constructor: " + className, e);
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException | ClassCastException e) {
            throw new IllegalArgumentException("Could not instantiate implementation: " + className, e);
        }
    }

    /**
     * Create a new BaseConfiguration object and set a comma delimiter handler, which interprets
     * comma-delimited values as a list of values.
     * @return
     */
    public static BaseConfiguration createBaseConfiguration() {
        final BaseConfiguration baseConfiguration = new BaseConfiguration();
        baseConfiguration.setListDelimiterHandler(COMMA_DELIMITER_HANDLER);
        return baseConfiguration;
    }

    /**
     * Load properties from a map object and return a MapConfiguration object. Comma-delimited values are interpreted as
     * a list of values.
     * @param configMap
     * @return
     */
    public static MapConfiguration loadMapConfiguration(Map<String, Object> configMap) {
        final MapConfiguration mapConfiguration = new MapConfiguration(configMap);
        mapConfiguration.setListDelimiterHandler(COMMA_DELIMITER_HANDLER);
        return mapConfiguration;
    }

    /**
     * Load properties from a given file name and return a PropertiesConfiguration object. Comma-delimited values are
     * interpreted as a list of values.
     * @param filename
     * @return
     * @throws ConfigurationException
     */
    public static PropertiesConfiguration loadPropertiesConfig(String filename) throws ConfigurationException {
        return loadPropertiesConfig(filename, true);
    }

    /**
     * Load properties from a given file name and return a PropertiesConfiguration object.
     * @param filename
     * @param setCommaDelimiterHandler
     * @return
     * @throws ConfigurationException
     */
    public static PropertiesConfiguration loadPropertiesConfig(String filename,
                                                               boolean setCommaDelimiterHandler) throws ConfigurationException {
        return loadPropertiesConfig(new Parameters().properties().setFileName(filename), setCommaDelimiterHandler);
    }

    /**
     * Load properties from a given file object and return a PropertiesConfiguration object. Comma-delimited values are
     * interpreted as a list of values.
     * @param file
     * @return
     * @throws ConfigurationException
     */
    public static PropertiesConfiguration loadPropertiesConfig(File file) throws ConfigurationException {
        return loadPropertiesConfig(new Parameters().properties().setFile(file), true);
    }

    private static PropertiesConfiguration loadPropertiesConfig(PropertiesBuilderParameters params,
                                                                boolean setCommaDelimiterHandler) throws ConfigurationException {
        FileBasedConfigurationBuilder<PropertiesConfiguration> builder =
            new FileBasedConfigurationBuilder<PropertiesConfiguration>(PropertiesConfiguration.class);
        PropertiesBuilderParameters newParams = params;
        if (setCommaDelimiterHandler) {
            newParams = newParams.setListDelimiterHandler(COMMA_DELIMITER_HANDLER);
        }
        return builder.configure(newParams).getConfiguration();
    }
}
