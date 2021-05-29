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

package org.janusgraph.graphdb.database.cache;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.janusgraph.diskstorage.EntryList;
import org.janusgraph.graphdb.types.system.BaseRelationType;

/**
 * This interface defines the methods that a SchemaCache must implement. A SchemaCache is maintained by the JanusGraph graph
 * database in order to make the frequent lookups of schema vertices and their attributes more efficient through a dedicated
 * caching layer. Schema vertices are type vertices and related vertices.
 *
 * The SchemaCache speeds up two types of lookups:
 * <ul>
 *     <li>Retrieving a type by its name (index lookup)</li>
 *     <li>Retrieving the relations of a schema vertex for predefined {@link org.janusgraph.graphdb.types.system.SystemRelationType}s</li>
 * </ul>
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface SchemaCache {

    Long getSchemaId(String schemaName);

    EntryList getSchemaRelations(long schemaId, BaseRelationType type, final Direction dir);

    void expireSchemaElement(final long schemaId);

    interface StoreRetrieval {

        Long retrieveSchemaByName(final String typeName);

        EntryList retrieveSchemaRelations(final long schemaId, final BaseRelationType type, final Direction dir);

    }

}
