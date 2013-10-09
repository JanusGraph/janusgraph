package com.thinkaurelius.titan.graphdb.types;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.KeyMaker;
import com.thinkaurelius.titan.core.TitanKey;
import com.thinkaurelius.titan.core.TitanType;
import com.thinkaurelius.titan.core.TypeMaker;
import com.thinkaurelius.titan.graphdb.database.IndexSerializer;
import com.thinkaurelius.titan.graphdb.relations.EdgeDirection;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;

import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static com.thinkaurelius.titan.graphdb.types.TypeAttributeType.DATATYPE;
import static com.thinkaurelius.titan.graphdb.types.TypeAttributeType.INDEXES;
import static com.tinkerpop.blueprints.Direction.IN;
import static com.tinkerpop.blueprints.Direction.OUT;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class StandardKeyMaker extends StandardTypeMaker implements KeyMaker {

    private Class<?> dataType;
    private Set<IndexType> indexes;

    public StandardKeyMaker(StandardTitanTx tx, IndexSerializer indexSerializer) {
        super(tx, indexSerializer);
        indexes = new HashSet<IndexType>(4);
        dataType = null;
        super.unique(Direction.OUT, UniquenessConsistency.NO_LOCK);
    }

    private IndexType[] checkIndexes(Set<IndexType> indexes) {
        IndexType[] result = new IndexType[indexes.size()];
        int i = 0;
        for (IndexType it : indexes) {
            Preconditions.checkArgument(isUnique(OUT) || (it.isStandardIndex() && it.getElementType() == Vertex.class),
                    "Only standard index is allowed on list property keys");
            Preconditions.checkArgument(indexSerializer.getIndexInformation(it.getIndexName()).supports(dataType), "" +
                    "Index [" + it.getIndexName() + "] does not support data type: " + dataType);
            result[i] = it;
            i++;
        }
        return result;
    }

    @Override
    public StandardKeyMaker dataType(Class<?> clazz) {
        Preconditions.checkArgument(clazz != null, "Need to specify a data type");
        dataType = clazz;
        return this;
    }

    @Override
    public KeyMaker list() {
        super.unique(Direction.OUT, null);
        return this;
    }

    @Override
    public KeyMaker single(UniquenessConsistency consistency) {
        super.unique(Direction.OUT, consistency);
        return this;
    }

    @Override
    public KeyMaker single() {
        single(UniquenessConsistency.LOCK);
        return this;
    }

    @Override
    public KeyMaker unique(UniquenessConsistency consistency) {
        super.unique(Direction.IN, consistency);
        return this;
    }

    @Override
    public KeyMaker unique() {
        unique(UniquenessConsistency.LOCK);
        return this;
    }

    @Override
    public StandardKeyMaker indexed(Class<? extends Element> clazz) {
        if (clazz == Element.class) {
            this.indexes.add(IndexType.of(Vertex.class));
            this.indexes.add(IndexType.of(Edge.class));
        } else {
            this.indexes.add(IndexType.of(clazz));
        }
        return this;
    }

    @Override
    public StandardKeyMaker indexed(String indexName, Class<? extends Element> clazz) {
        if (clazz == Element.class) {
            this.indexes.add(IndexType.of(indexName, Vertex.class));
            this.indexes.add(IndexType.of(indexName, Edge.class));
        } else {
            this.indexes.add(IndexType.of(indexName, clazz));
        }
        return this;
    }


    @Override
    public StandardKeyMaker hidden() {
        super.hidden();
        return this;
    }

    @Override
    public StandardKeyMaker unModifiable() {
        super.unModifiable();
        return this;
    }

    @Override
    public StandardKeyMaker makeStatic(Direction direction) {
        super.makeStatic(direction);
        return this;
    }

    @Override
    public StandardKeyMaker signature(TitanType... types) {
        throw new UnsupportedOperationException();
    }

    @Override
    public StandardKeyMaker sortKey(TitanType... types) {
        throw new UnsupportedOperationException();
    }

    @Override
    public TitanKey make() {
        Preconditions.checkArgument(dataType != null, "Need to specify a datatype");
        Preconditions.checkArgument(!dataType.isPrimitive(), "Primitive types are not supported. Use the corresponding object type, e.g. Integer.class instead of int.class [%s]", dataType);
        Preconditions.checkArgument(!dataType.isInterface(), "Datatype must be a class and not an interface: %s", dataType);
        Preconditions.checkArgument(dataType.isArray() || !Modifier.isAbstract(dataType.getModifiers()), "Datatype cannot be an abstract class: %s", dataType);
        Preconditions.checkArgument(!isUnique(IN) ||
                indexes.contains(IndexType.of(Vertex.class)), "A unique key must be indexed for vertices. Add 'indexed(Vertex.class)' to this key definition.");

        TypeAttribute.Map definition = makeDefinition();
        definition.setValue(DATATYPE, dataType).setValue(INDEXES, checkIndexes(indexes));
        return tx.makePropertyKey(getName(), definition);
    }
}
