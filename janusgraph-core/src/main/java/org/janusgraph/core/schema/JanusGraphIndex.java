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

import org.apache.tinkerpop.gremlin.structure.Element;
import org.janusgraph.core.PropertyKey;

/**
 * A JanusGraphIndex is an index installed on the graph in order to be able to efficiently retrieve graph elements
 * by their properties.
 * A JanusGraphIndex may either be a composite or mixed index and is created via {@link JanusGraphManagement#buildIndex(String, Class)}.
 * <p>
 * This interface allows introspecting an existing graph index. Existing graph indexes can be retrieved via
 * {@link JanusGraphManagement#getGraphIndex(String)} or {@link JanusGraphManagement#getGraphIndexes(Class)}.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface JanusGraphIndex extends Index {

    /**
     * Returns the name of the index
     * @return
     */
    String name();

    /**
     * Returns the name of the backing index. For composite indexes this returns a default name.
     * For mixed indexes, this returns the name of the configured indexing backend.
     *
     * @return
     */
    String getBackingIndex();

    /**
     * Returns which element type is being indexed by this index (vertex, edge, or property)
     *
     * @return
     */
    Class<? extends Element> getIndexedElement();

    /**
     * Returns the indexed keys of this index. If the returned array contains more than one element, its a
     * composite index.
     *
     * @return
     */
    PropertyKey[] getFieldKeys();

    /**
     * Returns the parameters associated with an indexed key of this index. Parameters modify the indexing
     * behavior of the underlying indexing backend.
     *
     * @param key
     * @return
     */
    Parameter[] getParametersFor(PropertyKey key);

    /**
     * Whether this is a unique index, i.e. values are uniquely associated with at most one element in the graph (for
     * a particular type)
     *
     * @return
     */
    boolean isUnique();

    /**
     * Returns the status of this index with respect to the provided {@link PropertyKey}.
     * For composite indexes, the key is ignored and the status of the index as a whole is returned.
     * For mixed indexes, the status of that particular key within the index is returned.
     *
     * @return
     */
    SchemaStatus getIndexStatus(PropertyKey key);

    /**
     * Whether this is a composite index
     * @return
     */
    boolean isCompositeIndex();

    /**
     * Whether this is a mixed index
     * @return
     */
    boolean isMixedIndex();


}
