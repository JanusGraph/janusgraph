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

package org.janusgraph.graphdb.types.indextype;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import org.janusgraph.core.*;
import org.janusgraph.core.schema.ConsistencyModifier;
import org.janusgraph.core.schema.Parameter;
import org.janusgraph.core.schema.SchemaStatus;
import org.janusgraph.graphdb.types.*;
import org.apache.tinkerpop.gremlin.structure.Direction;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class CompositeIndexTypeWrapper extends IndexTypeWrapper implements CompositeIndexType {

    public CompositeIndexTypeWrapper(SchemaSource base) {
        super(base);
    }

    @Override
    public boolean isCompositeIndex() {
        return true;
    }

    @Override
    public boolean isMixedIndex() {
        return false;
    }

    @Override
    public long getID() {
        return base.longId();
    }

    @Override
    public SchemaStatus getStatus() {
        return base.getStatus();
    }

    IndexField[] fields = null;

    @Override
    public IndexField[] getFieldKeys() {
        IndexField[] result = fields;
        if (result==null) {
            Iterable<SchemaSource.Entry> entries = base.getRelated(TypeDefinitionCategory.INDEX_FIELD,Direction.OUT);
            int numFields = Iterables.size(entries);
            result = new IndexField[numFields];
            for (SchemaSource.Entry entry : entries) {
                Integer value = ParameterType.INDEX_POSITION.findParameter((Parameter[]) entry.getModifier(),null);
                Preconditions.checkNotNull(value);
                int pos = value;
                Preconditions.checkArgument(pos>=0 && pos<numFields,"Invalid field position: %s",pos);
                assert entry.getSchemaType() instanceof PropertyKey;
                result[pos]=IndexField.of((PropertyKey)entry.getSchemaType());
            }
            fields=result;
        }
        assert result!=null;
        return result;
    }

    @Override
    public void resetCache() {
        super.resetCache();
        fields = null;
    }

    @Override
    public Cardinality getCardinality() {
        return base.getDefinition().getValue(TypeDefinitionCategory.INDEX_CARDINALITY,Cardinality.class);
    }

    private ConsistencyModifier consistency = null;

    public ConsistencyModifier getConsistencyModifier() {
        if (consistency==null) {
            consistency = TypeUtil.getConsistencyModifier(base);
        }
        return consistency;
    }
}
