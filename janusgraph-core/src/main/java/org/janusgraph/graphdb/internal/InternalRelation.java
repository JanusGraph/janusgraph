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

package org.janusgraph.graphdb.internal;

import org.janusgraph.core.JanusGraphRelation;
import org.janusgraph.core.PropertyKey;

/**
 * Internal Relation interface adding methods that should only be used by JanusGraph.
 *
 * The "direct" qualifier in the method names indicates that the corresponding action is executed on this relation
 * object and not migrated to a different transactional context. It also means that access returns the "raw" value of
 * what is stored on this relation
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface InternalRelation extends JanusGraphRelation, InternalElement {

    /**
     * Returns this relation in the current transactional context
     *
     * @return
     */
    @Override
    InternalRelation it();

    /**
     * Returns the vertex at the given position (0=OUT, 1=IN) of this relation
     * @param pos
     * @return
     */
    InternalVertex getVertex(int pos);

    /**
     * Number of vertices on this relation.
     *
     * @return
     */
    int getArity();

    /**
     * Number of vertices on this relation that are aware of its existence. This number will
     * differ from {@link #getArity()}
     *
     */
    int getLen();



    <O> O getValueDirect(PropertyKey key);

    void setPropertyDirect(PropertyKey key, Object value);

    Iterable<PropertyKey> getPropertyKeysDirect();

    <O> O removePropertyDirect(PropertyKey key);

}
