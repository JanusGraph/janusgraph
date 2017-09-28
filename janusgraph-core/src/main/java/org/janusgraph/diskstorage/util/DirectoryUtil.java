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

package org.janusgraph.diskstorage.util;

import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.PermanentBackendException;

import java.io.File;

/**
 * Utility methods for dealing with directory structures that are not provided by Apache Commons.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class DirectoryUtil {


    public static File getOrCreateDataDirectory(String location) throws BackendException {
        return getOrCreateDataDirectory(location, null);
    }

    public static File getOrCreateDataDirectory(String location, String childLocation) throws BackendException {
        final File storageDir;
        if (null != childLocation) {
            storageDir = new File(location, childLocation);
        } else {
            storageDir = new File(location);
        }
        if (storageDir.exists() && storageDir.isFile())
            throw new PermanentBackendException(String.format("%s exists but is a file.", location));

        if (!storageDir.exists() && !storageDir.mkdirs())
            throw new PermanentBackendException(String.format("Failed to create directory %s for local storage.", location));

        return storageDir;
    }

}
