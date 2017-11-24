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

package org.janusgraph.graphdb.idmanagement;

/**
 * Interface for determining the type of element a particular id refers to.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface IDInspector {

    boolean isSchemaVertexId(long id);

    boolean isRelationTypeId(long id);

    boolean isEdgeLabelId(long id);

    boolean isPropertyKeyId(long id);

    boolean isSystemRelationTypeId(long id);

    boolean isVertexLabelVertexId(long id);

    boolean isGenericSchemaVertexId(long id);

    boolean isUserVertexId(long id);

    boolean isUnmodifiableVertex(long id);

    boolean isPartitionedVertex(long id);

    long getCanonicalVertexId(long partitionedVertexId);

}
