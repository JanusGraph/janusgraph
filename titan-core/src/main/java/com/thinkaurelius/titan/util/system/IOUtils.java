package com.thinkaurelius.titan.util.system;

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

    static public boolean deleteFromDirectory(File path) {
        return deleteDirectory(path, false);
    }

    static public boolean deleteDirectory(File path, boolean includeDir) {
        boolean success = true;
        if (path.exists()) {
            File[] files = path.listFiles();
            for (int i = 0; i < files.length; i++) {
                if (files[i].isDirectory()) {
                    success = deleteDirectory(files[i], true) && success;
                } else {
                    success = files[i].delete() && success;
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
}
