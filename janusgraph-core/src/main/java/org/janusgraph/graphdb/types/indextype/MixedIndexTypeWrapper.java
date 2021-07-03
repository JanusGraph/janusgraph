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

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.Parameter;
import org.janusgraph.graphdb.types.MixedIndexType;
import org.janusgraph.graphdb.types.ParameterIndexField;
import org.janusgraph.graphdb.types.SchemaSource;
import org.janusgraph.graphdb.types.TypeDefinitionCategory;

import java.util.List;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class MixedIndexTypeWrapper extends IndexTypeWrapper implements MixedIndexType {

    public static final String NAME_PREFIX = "extindex";
    private ParameterIndexField[] fields = null;

    public MixedIndexTypeWrapper(SchemaSource base) {
        super(base);
    }

    @Override
    public boolean isCompositeIndex() {
        return false;
    }

    @Override
    public boolean isMixedIndex() {
        return true;
    }

    @Override
    public ParameterIndexField[] getFieldKeys() {
        ParameterIndexField[] result = fields;
        if (result==null) {
            List<SchemaSource.Entry> entries = base.getRelated(TypeDefinitionCategory.INDEX_FIELD,Direction.OUT);
            int numFields = entries.size();
            result = new ParameterIndexField[numFields];
            int pos = 0;
            for (SchemaSource.Entry entry : entries) {
                assert entry.getSchemaType() instanceof PropertyKey;
                assert entry.getModifier() instanceof Parameter[];
                result[pos++]=ParameterIndexField.of((PropertyKey)entry.getSchemaType(),(Parameter[])entry.getModifier());
            }
            fields = result;
        }
        assert result!=null;
        return result;
    }

    @Override
    public ParameterIndexField getField(PropertyKey key) {
        return (ParameterIndexField)super.getField(key);
    }

    @Override
    public void resetCache() {
        super.resetCache();
        fields = null;
    }

    @Override
    public String getStoreName() {
        return base.getDefinition().getValue(TypeDefinitionCategory.INDEXSTORE_NAME,String.class);
    }

}
