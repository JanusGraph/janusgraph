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

package org.janusgraph.hadoop.formats.util.input;

import org.janusgraph.graphdb.database.RelationReader;
import org.janusgraph.graphdb.database.serialize.Serializer;
import org.janusgraph.graphdb.idmanagement.IDManager;
import org.janusgraph.graphdb.types.TypeInspector;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface JanusGraphHadoopSetup {

    TypeInspector getTypeInspector();

    SystemTypeInspector getSystemTypeInspector();

    RelationReader getRelationReader();

    IDManager getIDManager();

    void close();

    boolean getFilterPartitionedVertices();

    /**
     * Whether edge ids carry the edge's sort-key values, as configured by
     * {@link org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration#RELATION_ID_SORT_KEY}.
     */
    default boolean hasRelationIdSortKey() {
        return false;
    }

    /**
     * The graph's attribute serializer, needed to encode sort-key values when {@link #hasRelationIdSortKey()}.
     */
    default Serializer getDataSerializer() {
        throw new UnsupportedOperationException();
    }

}
