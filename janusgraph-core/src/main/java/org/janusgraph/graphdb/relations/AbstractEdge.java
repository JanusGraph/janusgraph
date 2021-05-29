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

package org.janusgraph.graphdb.relations;

import com.google.common.base.Preconditions;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.janusgraph.core.EdgeLabel;
import org.janusgraph.core.JanusGraphEdge;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.graphdb.internal.InternalVertex;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public abstract class AbstractEdge extends AbstractTypedRelation implements JanusGraphEdge {

    private InternalVertex start;
    private InternalVertex end;

    public AbstractEdge(long id, EdgeLabel label, InternalVertex start, InternalVertex end) {
        super(id, label);

        assert start != null && end != null;
        this.start = start;
        this.end = end;
    }

    @Override
    public String toString() {
        return StringFactory.edgeString(this);
    }

    @Override
    public String label() {
        return type.name();
    }

    public void setVertexAt(int pos, InternalVertex vertex) {
        Preconditions.checkArgument(vertex != null && getVertex(pos).equals(vertex));
        switch (pos) {
            case 0:
                start = vertex;
                break;
            case 1:
                end = vertex;
                break;
            default:
                throw new IllegalArgumentException("Invalid position: " + pos);
        }
    }

    @Override
    public InternalVertex getVertex(int pos) {
        switch (pos) {
            case 0:
                return start;
            case 1:
                return end;
            default:
                throw new IllegalArgumentException("Invalid position: " + pos);
        }
    }

    @Override
    public int getArity() {
        return 2;
    }

    @Override
    public int getLen() {
        assert !type.isUnidirected(Direction.IN);
        return type.isUnidirected(Direction.OUT)?1:2;
    }

    @Override
    public JanusGraphVertex vertex(Direction dir) {
        return getVertex(EdgeDirection.position(dir));
    }


    @Override
    public JanusGraphVertex otherVertex(Vertex vertex) {
        if (start.equals(vertex))
            return end;

        if (end.equals(vertex))
            return start;

        throw new IllegalArgumentException("Edge is not incident on vertex");
    }

    @Override
    public boolean isProperty() {
        return false;
    }

    @Override
    public boolean isEdge() {
        return true;
    }


}
