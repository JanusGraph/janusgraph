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
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.janusgraph.core.JanusGraphElement;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.RelationType;
import org.janusgraph.core.attribute.Cmp;
import org.janusgraph.core.schema.Parameter;
import org.janusgraph.core.schema.SchemaStatus;
import org.janusgraph.graphdb.database.IndexSerializer;
import org.janusgraph.graphdb.query.condition.Condition;
import org.janusgraph.graphdb.query.condition.PredicateCondition;
import org.janusgraph.graphdb.types.MixedIndexType;
import org.janusgraph.graphdb.types.ParameterIndexField;
import org.janusgraph.graphdb.types.SchemaSource;
import org.janusgraph.graphdb.types.TypeDefinitionCategory;

import java.util.Arrays;
import java.util.List;
import java.util.stream.StreamSupport;

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

    @Override
    public <E extends JanusGraphElement> boolean coversAll(Condition<E> condition, IndexSerializer indexInfo) {
        if (condition.getType()!=Condition.Type.LITERAL) {
            return StreamSupport.stream(condition.getChildren().spliterator(), false)
                .allMatch(child -> coversAll(child, indexInfo));
        }
        if (!(condition instanceof PredicateCondition)) return false;
        final PredicateCondition<RelationType, E> atom = (PredicateCondition<RelationType, E>) condition;
        if (atom.getValue() == null && atom.getPredicate() != Cmp.NOT_EQUAL) return false;

        Preconditions.checkArgument(atom.getKey().isPropertyKey());
        final PropertyKey key = (PropertyKey) atom.getKey();
        final ParameterIndexField[] fields = getFieldKeys();
        final ParameterIndexField match = Arrays.stream(fields)
            .filter(field -> field.getStatus() == SchemaStatus.ENABLED)
            .filter(field -> field.getFieldKey().equals(key))
            .findAny().orElse(null);

        if (match == null) return false;
        boolean existsQuery = atom.getValue() == null && atom.getPredicate() == Cmp.NOT_EQUAL && indexInfo.supportsExistsQuery(this, match);
        return existsQuery || indexInfo.supports(this, match, atom.getPredicate());
    }
}
