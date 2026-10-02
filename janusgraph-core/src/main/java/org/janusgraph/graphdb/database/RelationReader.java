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

package org.janusgraph.graphdb.database;

import org.janusgraph.diskstorage.Entry;
import org.janusgraph.graphdb.relations.RelationCache;
import org.janusgraph.graphdb.types.TypeInspector;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface RelationReader {

    /**
     * Parses a relation entry.
     *
     * @param parseHeaderOnly whether to parse only the type, direction, id and other end of the relation and leave
     *                        its properties out. Even then the parse is complete, {@link RelationCache#isFullyParsed()},
     *                        when the relation can have no property: no bytes follow the header, its type has neither a
     *                        sort key nor a signature, and the entry carries no metadata which stands for a property
     */
    RelationCache parseRelation(Entry data, boolean parseHeaderOnly, TypeInspector tx);

}
