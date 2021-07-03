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
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.JanusGraphSchemaType;
import org.janusgraph.graphdb.internal.ElementCategory;
import org.janusgraph.graphdb.types.IndexField;
import org.janusgraph.graphdb.types.IndexType;
import org.janusgraph.graphdb.types.SchemaSource;
import org.janusgraph.graphdb.types.TypeDefinitionCategory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public abstract class IndexTypeWrapper implements IndexType {

    protected final SchemaSource base;
    private volatile Map<PropertyKey,IndexField> fieldMap = null;
    private volatile boolean cachedTypeConstraint = false;
    private volatile JanusGraphSchemaType schemaTypeConstraint = null;

    public IndexTypeWrapper(SchemaSource base) {
        Preconditions.checkNotNull(base);
        this.base = base;
    }

    public SchemaSource getSchemaBase() {
        return base;
    }

    @Override
    public ElementCategory getElement() {
        return base.getDefinition().getValue(TypeDefinitionCategory.ELEMENT_CATEGORY,ElementCategory.class);
    }

    @Override
    public int hashCode() {
        return base.hashCode();
    }

    @Override
    public boolean equals(Object oth) {
        return this==oth || (oth instanceof IndexTypeWrapper && base.equals(((IndexTypeWrapper)oth).base));
    }

    @Override
    public String toString() {
        return base.name();
    }

    @Override
    public String getName() {
        return base.name();
    }

    @Override
    public IndexField getField(PropertyKey key) {
        Map<PropertyKey,IndexField> result = fieldMap;
        if (result==null) {
            IndexField[] fieldKeys = getFieldKeys();
            result = new HashMap<>(fieldKeys.length);
            for (IndexField f : fieldKeys) {
                result.put(f.getFieldKey(),f);
            }
            result= Collections.unmodifiableMap(result);
            fieldMap=result;
        }
        assert result!=null;
        return result.get(key);
    }

    @Override
    public boolean hasSchemaTypeConstraint() {
        return getSchemaTypeConstraint()!=null;
    }

    @Override
    public JanusGraphSchemaType getSchemaTypeConstraint() {
        JanusGraphSchemaType constraint;
        if (!cachedTypeConstraint) {
            List<SchemaSource.Entry> related = base.getRelated(TypeDefinitionCategory.INDEX_SCHEMA_CONSTRAINT, Direction.OUT);
            if (related.isEmpty()) {
                constraint=null;
            } else {
                constraint =
                        (JanusGraphSchemaType)Iterables.getOnlyElement(related).getSchemaType();
                assert constraint!=null;
            }
            schemaTypeConstraint = constraint;
            cachedTypeConstraint = true;
        } else {
            constraint = schemaTypeConstraint;
        }
        return constraint;
    }

    @Override
    public void resetCache() {
        base.resetCache();
        fieldMap=null;
    }

    @Override
    public boolean indexesKey(PropertyKey key) {
        return getField(key)!=null;
    }

    @Override
    public String getBackingIndexName() {
        return base.getDefinition().getValue(TypeDefinitionCategory.BACKING_INDEX,String.class);
    }

}
