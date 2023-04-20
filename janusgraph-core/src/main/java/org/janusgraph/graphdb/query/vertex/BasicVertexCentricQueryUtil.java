// Copyright 2023 JanusGraph Authors
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

package org.janusgraph.graphdb.query.vertex;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.VertexLabel;
import org.janusgraph.graphdb.types.VertexLabelVertex;
import org.janusgraph.graphdb.types.system.BaseLabel;
import org.janusgraph.graphdb.types.system.BaseVertexLabel;

public class BasicVertexCentricQueryUtil {

    private BasicVertexCentricQueryUtil() {}

    public static <Q extends BasicVertexCentricQueryBuilder<? extends BasicVertexCentricQueryBuilder<?>>> Q withLabelVertices(Q queryBuilder){
        queryBuilder.noPartitionRestriction().type(BaseLabel.VertexLabelEdge).direction(Direction.OUT);
        return queryBuilder;
    }

    public static VertexLabel castToVertexLabel(Vertex vertexLabel){
        if (vertexLabel==null) return BaseVertexLabel.DEFAULT_VERTEXLABEL;
        else return (VertexLabelVertex)vertexLabel;
    }

}
