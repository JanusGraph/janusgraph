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

import org.janusgraph.diskstorage.keycolumnvalue.SliceQuery;
import org.janusgraph.graphdb.database.RelationReader;
import org.janusgraph.graphdb.idmanagement.IDManager;
import org.janusgraph.graphdb.types.TypeInspector;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface JanusGraphHadoopSetup {

    TypeInspector getTypeInspector();

    SystemTypeInspector getSystemTypeInspector();

    RelationReader getRelationReader(long vertexId);

    IDManager getIDManager();

    /**
     * Return an input slice across the entire row.
     *
     * TODO This would ideally slice only columns inside the row needed by the query.
     * The slice must include the hidden vertex state property (to filter removed vertices).
     *
     */
    SliceQuery inputSlice();

    void close();

    boolean getFilterPartitionedVertices();

}
