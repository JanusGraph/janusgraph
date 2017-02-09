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

package org.janusgraph.hadoop.compat;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import org.apache.hadoop.util.VersionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HadoopCompatLoader {

    private static final Logger log =
            LoggerFactory.getLogger(HadoopCompatLoader.class);

    public static final HadoopCompat DEFAULT_COMPAT = getCompat();

    // TODO add a string argument that allows specifying a class instead of relying heuristics around VersionInfo.getVersion()
    // TODO add threadsafe caching that is aware of the string argument and instantiates a compat for each argument at most once (assuming the instantiation succeeds)
    public static HadoopCompat getCompat() {
        String ver = VersionInfo.getVersion();

        log.debug("Read Hadoop VersionInfo string {}", ver);

        final String pkgName = HadoopCompatLoader.class.getPackage().getName();
        final String className;

        if (ver.startsWith("1.")) {
            className = pkgName + ".h1.Hadoop1Compat";
        } else {
            className = pkgName + ".h2.Hadoop2Compat";
        }

        log.debug("Attempting to load class {} and instantiate with nullary constructor", className);
        try {
            Constructor<?> ctor = Class.forName(className).getConstructor();
            log.debug("Invoking constructor {}", ctor);
            return (HadoopCompat)ctor.newInstance();
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        } catch (SecurityException e) {
            throw new RuntimeException(e);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        } catch (InstantiationException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        } catch (IllegalArgumentException e) {
            throw new RuntimeException(e);
        } catch (InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }
}
