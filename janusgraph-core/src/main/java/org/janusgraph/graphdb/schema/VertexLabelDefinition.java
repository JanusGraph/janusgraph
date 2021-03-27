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

import org.janusgraph.core.VertexLabel;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 *
 * @deprecated part of the management revamp in JG, see https://github.com/JanusGraph/janusgraph/projects/3.
 */
@Deprecated
public class VertexLabelDefinition extends SchemaElementDefinition {

    private final boolean isPartitioned;
    private final boolean isStatic;

    public VertexLabelDefinition(String name, long id, boolean isPartitioned, boolean isStatic) {
        super(name, id);
        this.isPartitioned = isPartitioned;
        this.isStatic = isStatic;
    }

    public VertexLabelDefinition(VertexLabel vl) {
        this(vl.name(),vl.longId(),vl.isPartitioned(),vl.isStatic());
    }

    public boolean isStatic() {
        return isStatic;
    }

    public boolean isPartitioned() {
        return isPartitioned;
    }

    public boolean hasDefaultConfiguration() {
        return !isPartitioned && !isStatic;
    }

}
