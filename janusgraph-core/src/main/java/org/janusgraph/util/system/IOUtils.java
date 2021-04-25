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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;

/**
 * IO Utility class
 *
 */
public class IOUtils {
    private static final Logger logger = LoggerFactory.getLogger(IOUtils.class);

    public static boolean deleteFromDirectory(File path) {
        return deleteDirectory(path, false);
    }

    public static boolean deleteDirectory(File path, boolean includeDir) {
        boolean success = true;
        if (path.exists()) {
            File[] files = path.listFiles();
            for (File file : files) {
                if (file.isDirectory()) {
                    success = deleteDirectory(file, true) && success;
                } else {
                    success = file.delete() && success;
                }
            }
        }
        if (includeDir) success = path.delete() && success;
        return success;
    }

    public static void closeQuietly(Closeable c) {
        try {
            if (c != null)
                c.close();
        } catch (Exception e) {
            logger.warn("Failed closing " + c, e);
        }
    }

    public static void closeQuietly(AutoCloseable c) {

        try {
            if (c != null)
                c.close();
        } catch (Exception e) {
            logger.warn("Failed closing " + c, e);
        }
    }
}
