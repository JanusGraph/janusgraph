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

    public boolean isSchemaVertexId(long id);

    public boolean isRelationTypeId(long id);

    public boolean isEdgeLabelId(long id);

    public boolean isPropertyKeyId(long id);

    public boolean isSystemRelationTypeId(long id);

    public boolean isVertexLabelVertexId(long id);

    public boolean isGenericSchemaVertexId(long id);

    public boolean isUserVertexId(long id);

    public boolean isUnmodifiableVertex(long id);

    public boolean isPartitionedVertex(long id);

    public long getCanonicalVertexId(long partitionedVertexId);

}
