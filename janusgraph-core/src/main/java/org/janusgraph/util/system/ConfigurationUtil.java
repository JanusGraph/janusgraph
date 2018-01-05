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
import org.apache.commons.configuration.Configuration;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.*;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class ConfigurationUtil {

    private static final char CONFIGURATION_SEPARATOR = '.';

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

}
