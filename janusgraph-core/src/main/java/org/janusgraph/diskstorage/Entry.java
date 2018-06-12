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

package org.janusgraph.diskstorage;

import org.janusgraph.graphdb.relations.RelationCache;

/**
 * An entry is the primitive persistence unit used in the graph database storage backend.
 * <p>
 * An entry consists of a column and value both of which are general {@link java.nio.ByteBuffer}s.
 * The value may be null but the column may not.
 *
 * @author Matthias Br&ouml;cheler (me@matthiasb.com);
 */
public interface Entry extends StaticBuffer, MetaAnnotated {

    int getValuePosition();

    boolean hasValue();

    StaticBuffer getColumn();

    <T> T getColumnAs(Factory<T> factory);

    StaticBuffer getValue();

    <T> T getValueAs(Factory<T> factory);

    /**
     * Returns the cached parsed representation of this Entry if it exists, else NULL
     *
     * @return
     */
    RelationCache getCache();

    /**
     * Sets the cached parsed representation of this Entry. This method does not synchronize,
     * so a previously set representation would simply be overwritten.
     *
     * @param cache
     */
    void setCache(RelationCache cache);
}
