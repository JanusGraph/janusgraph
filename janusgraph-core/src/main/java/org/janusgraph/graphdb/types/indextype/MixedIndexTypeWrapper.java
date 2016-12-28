package com.thinkaurelius.titan.graphdb.types.indextype;

import com.google.common.collect.Iterables;
import com.thinkaurelius.titan.core.schema.Parameter;
import com.thinkaurelius.titan.core.PropertyKey;
import com.thinkaurelius.titan.graphdb.types.*;
import org.apache.tinkerpop.gremlin.structure.Direction;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class MixedIndexTypeWrapper extends IndexTypeWrapper implements MixedIndexType {

    public static final String NAME_PREFIX = "extindex";

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

    ParameterIndexField[] fields = null;

    @Override
    public ParameterIndexField[] getFieldKeys() {
        ParameterIndexField[] result = fields;
        if (result==null) {
            Iterable<SchemaSource.Entry> entries = base.getRelated(TypeDefinitionCategory.INDEX_FIELD,Direction.OUT);
            int numFields = Iterables.size(entries);
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
