package org.janusgraph.graphdb.types.system;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.Multiplicity;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.ConsistencyModifier;
import org.janusgraph.core.schema.SchemaStatus;
import org.janusgraph.core.schema.JanusSchemaType;
import org.janusgraph.graphdb.internal.ElementCategory;
import org.janusgraph.graphdb.internal.JanusSchemaCategory;
import org.janusgraph.graphdb.internal.Token;
import org.janusgraph.graphdb.types.CompositeIndexType;
import org.janusgraph.graphdb.types.IndexField;
import org.janusgraph.graphdb.types.IndexType;
import org.janusgraph.graphdb.types.TypeDefinitionDescription;
import org.apache.tinkerpop.gremlin.structure.Direction;

import java.util.Collections;

public class BaseKey extends BaseRelationType implements PropertyKey {

    private enum Index { NONE, STANDARD, UNIQUE }

    //We rely on the vertex-existence property to be the smallest (in byte-order) when iterating over the entire graph
    public static final BaseKey VertexExists =
            new BaseKey("VertexExists", Boolean.class, 1, Index.NONE, Cardinality.SINGLE);

    public static final BaseKey SchemaName =
            new BaseKey("SchemaName", String.class, 32, Index.UNIQUE, Cardinality.SINGLE);

    public static final BaseKey SchemaDefinitionProperty =
            new BaseKey("SchemaDefinitionProperty", Object.class, 33, Index.NONE, Cardinality.LIST);

    public static final BaseKey SchemaCategory =
            new BaseKey("SchemaCategory", JanusSchemaCategory.class, 34, Index.STANDARD, Cardinality.SINGLE);

    public static final BaseKey SchemaDefinitionDesc =
            new BaseKey("SchemaDefinitionDescription", TypeDefinitionDescription.class, 35, Index.NONE, Cardinality.SINGLE);

    public static final BaseKey SchemaUpdateTime =
            new BaseKey("SchemaUpdateTimestamp", Long.class, 36, Index.NONE, Cardinality.SINGLE);



    private final Class<?> dataType;
    private final Index index;
    private final Cardinality cardinality;

    private BaseKey(String name, Class<?> dataType, int id, Index index, Cardinality cardinality) {
        super(name, id, JanusSchemaCategory.PROPERTYKEY);
        Preconditions.checkArgument(index!=null && cardinality!=null);
        this.dataType = dataType;
        this.index = index;
        this.cardinality = cardinality;
    }

    @Override
    public Class<?> dataType() {
        return dataType;
    }

    @Override
    public final boolean isPropertyKey() {
        return true;
    }

    @Override
    public final boolean isEdgeLabel() {
        return false;
    }

    @Override
    public Multiplicity multiplicity() {
        return Multiplicity.convert(cardinality());
    }

    @Override
    public boolean isUnidirected(Direction dir) {
        return dir==Direction.OUT;
    }

    @Override
    public Cardinality cardinality() {
        return cardinality;
    }

    @Override
    public Iterable<IndexType> getKeyIndexes() {
        if (index==Index.NONE) return Collections.EMPTY_LIST;
        return ImmutableList.of((IndexType)indexDef);
    }

    private final CompositeIndexType indexDef = new CompositeIndexType() {

        private final IndexField[] fields = {IndexField.of(BaseKey.this)};
//        private final Set<JanusKey> fieldSet = ImmutableSet.of((JanusKey)SystemKey.this);

        @Override
        public String toString() {
            return getName();
        }

        @Override
        public long getID() {
            return BaseKey.this.longId();
        }

        @Override
        public IndexField[] getFieldKeys() {
            return fields;
        }

        @Override
        public IndexField getField(PropertyKey key) {
            if (key.equals(BaseKey.this)) return fields[0];
            else return null;
        }

        @Override
        public boolean indexesKey(PropertyKey key) {
            return getField(key)!=null;
        }

        @Override
        public Cardinality getCardinality() {
            switch(index) {
                case UNIQUE: return Cardinality.SINGLE;
                case STANDARD: return Cardinality.LIST;
                default: throw new AssertionError();
            }
        }

        @Override
        public ConsistencyModifier getConsistencyModifier() {
            return ConsistencyModifier.LOCK;
        }

        @Override
        public ElementCategory getElement() {
            return ElementCategory.VERTEX;
        }

        @Override
        public boolean hasSchemaTypeConstraint() {
            return false;
        }

        @Override
        public JanusSchemaType getSchemaTypeConstraint() {
            return null;
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
        public String getBackingIndexName() {
            return Token.INTERNAL_INDEX_NAME;
        }

        @Override
        public String getName() {
            return "SystemIndex#"+BaseKey.this.name();
        }

        @Override
        public SchemaStatus getStatus() {
            return SchemaStatus.ENABLED;
        }

        @Override
        public void resetCache() {}

        //Use default hashcode and equals
    };

}
