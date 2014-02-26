package com.thinkaurelius.titan.graphdb.types.indextype;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.thinkaurelius.titan.core.Parameter;
import com.thinkaurelius.titan.core.ParameterType;
import com.thinkaurelius.titan.core.TitanKey;
import com.thinkaurelius.titan.graphdb.internal.ElementCategory;
import com.thinkaurelius.titan.graphdb.internal.Token;
import com.thinkaurelius.titan.graphdb.types.*;
import com.tinkerpop.blueprints.Direction;

import java.util.Set;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class ExternalIndexTypeWrapper extends IndexTypeWrapper implements ExternalIndexType {

    public static final String NAME_PREFIX = "extindex";

    public ExternalIndexTypeWrapper(TypeSource base) {
        super(base);
    }

    @Override
    public boolean isInternalIndex() {
        return false;
    }

    @Override
    public boolean isExternalIndex() {
        return true;
    }

    ParameterIndexField[] fields = null;

    @Override
    public ParameterIndexField[] getFields() {
        if (fields==null) {
            Iterable<TypeSource.Entry> entries = base.getRelated(TypeDefinitionCategory.INDEX_FIELD,Direction.OUT);
            int numFields = Iterables.size(entries);
            ParameterIndexField[] f = new ParameterIndexField[numFields];
            int pos = 0;
            for (TypeSource.Entry entry : entries) {
                assert entry.getSchemaType() instanceof TitanKey;
                assert entry.getModifier() instanceof Parameter[];
                f[pos++]=ParameterIndexField.of((TitanKey)entry.getSchemaType(),(Parameter[])entry.getModifier());
            }
            fields=f;
        }
        assert fields!=null;
        return fields;
    }

    @Override
    public ParameterIndexField getField(TitanKey key) {
        return (ParameterIndexField)super.getField(key);
    }

    @Override
    public String getIndexName() {
        String name = base.getName();
        Preconditions.checkArgument(name.startsWith(NAME_PREFIX+Token.SEPARATOR_CHAR));
        name = name.substring((NAME_PREFIX).length()+1);
        int pos = name.indexOf(Token.SEPARATOR_CHAR);
        Preconditions.checkArgument(pos>0);
        assert getElement()== ElementCategory.getByName(name.substring(pos+1));
        return name.substring(0, pos);
    }

    public static String getExternalIndexName(String indexName, ElementCategory category) {
        return NAME_PREFIX + Token.SEPARATOR_CHAR + indexName + Token.SEPARATOR_CHAR + category.getName();
    }


}
