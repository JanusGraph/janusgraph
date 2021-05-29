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

package org.janusgraph.graphdb.schema;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.janusgraph.core.EdgeLabel;
import org.janusgraph.core.Multiplicity;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 *
 * @deprecated part of the management revamp in JG, see https://github.com/JanusGraph/janusgraph/projects/3.
 */
@Deprecated
public class EdgeLabelDefinition extends RelationTypeDefinition {

    private final boolean unidirected;

    public EdgeLabelDefinition(String name, long id, Multiplicity multiplicity, boolean unidirected) {
        super(name, id, multiplicity);
        this.unidirected = unidirected;
    }

    public EdgeLabelDefinition(EdgeLabel label) {
        this(label.name(),label.longId(),label.multiplicity(),label.isUnidirected());
    }

    public boolean isDirected() {
        return !unidirected;
    }

    public boolean isUnidirected() {
        return unidirected;
    }

    @Override
    public boolean isUnidirected(Direction dir) {
        if (unidirected) return dir==Direction.OUT;
        else return dir==Direction.BOTH;
    }



}
