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

package org.janusgraph.core.schema;

/**
 * Used to read and change the global JanusGraph configuration.
 * The configuration options are read from the graph and affect the entire database.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface JanusGraphConfiguration {

    /**
     * Returns a string representation of the provided configuration option or namespace for inspection.
     * <p>
     * An exception is thrown if the path is invalid.
     *
     * @param path
     * @return
     */
    String get(String path);

    /**
     * Sets the configuration option identified by the provided path to the given value.
     *
     * @param path
     * @param value
     */
    JanusGraphConfiguration set(String path, Object value);

}
