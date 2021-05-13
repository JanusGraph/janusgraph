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

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 *
 * @deprecated part of the management revamp in JG, see https://github.com/JanusGraph/janusgraph/projects/3.
 */
@Deprecated
public class SchemaElementDefinition {

    private final String name;
    private final long id;


    public SchemaElementDefinition(String name, long id) {
        Preconditions.checkArgument(StringUtils.isNotBlank(name), "name cannot be blank");
        this.name = name;
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public long getLongId() {
        return id;
    }


    @Override
    public int hashCode() {
        return name.hashCode();
    }

    @Override
    public boolean equals(Object oth) {
        if (this==oth) return true;
        else if (oth==null || !getClass().isInstance(oth)) return false;
        return name.equals(((SchemaElementDefinition)oth).name);
    }

    @Override
    public String toString() {
        return name;
    }

}
