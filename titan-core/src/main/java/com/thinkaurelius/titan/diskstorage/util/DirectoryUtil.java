package com.thinkaurelius.titan.diskstorage.util;

import com.thinkaurelius.titan.diskstorage.BackendException;
import com.thinkaurelius.titan.diskstorage.PermanentBackendException;

import java.io.File;

/**
 * Utility methods for dealing with directory structures that are not provided by Apache Commons.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class DirectoryUtil {


    public static File getOrCreateDataDirectory(String location) throws BackendException {
        File storageDir = new File(location);

        if (storageDir.exists() && storageDir.isFile())
            throw new PermanentBackendException(String.format("%s exists but is a file.", location));

        if (!storageDir.exists() && !storageDir.mkdirs())
            throw new PermanentBackendException(String.format("Failed to create directory %s for local storage.", location));

        return storageDir;
    }

}
