/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.bootstrap;

import org.elasticsearch.common.io.PathUtils;

import java.net.MalformedURLException;
import java.net.URL;

/**
 * This class masks the Elasticsearch class of the same name.
 * Clients are responsible for ensuring their classpath is sane.
 */
public class JarHell {

    public static void checkJarHell() { }

    public static URL[] parseClassPath()  {
        return parseClassPath(System.getProperty("java.class.path"));
    }

    static URL[] parseClassPath(String classPath) {
        String pathSeparator = System.getProperty("path.separator");
        String fileSeparator = System.getProperty("file.separator");
        String elements[] = classPath.split(pathSeparator);
        URL urlElements[] = new URL[elements.length];
        for (int i = 0; i < elements.length; i++) {
            String element = elements[i];
            if (element.isEmpty()) {
                throw new IllegalStateException("Classpath should not contain empty elements! (outdated shell script from a previous version?) classpath='" + classPath + "'");
            }
            if (element.startsWith("/") && "\\".equals(fileSeparator)) {
                element = element.replace("/", "\\");
                if (element.length() >= 3 && element.charAt(2) == ':') {
                    element = element.substring(1);
                }
            }
            try {
                urlElements[i] = PathUtils.get(element).toUri().toURL();
            } catch (MalformedURLException e) {
                throw new RuntimeException(e);
            }
        }
        return urlElements;
    }

    public static void checkJarHell(URL urls[]) { }

    public static void checkVersionFormat(String targetVersion) { }

    public static void checkJavaVersion(String resource, String targetVersion) { }

}
