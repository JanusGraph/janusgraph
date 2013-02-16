package com.thinkaurelius.titan.graphdb.types.manager;

import com.thinkaurelius.titan.core.TitanKey;
import com.thinkaurelius.titan.core.TitanLabel;
import com.thinkaurelius.titan.core.TitanType;
import com.thinkaurelius.titan.core.TypeGroup;
import com.thinkaurelius.titan.core.TypeMaker;
import com.thinkaurelius.titan.graphdb.transaction.InternalTitanTransaction;
import com.thinkaurelius.titan.graphdb.types.*;

import static com.thinkaurelius.titan.graphdb.types.manager.TypeManagerUtil.convertSignature;

public class InMemoryTypeManager implements TypeManager {

    private final TypeFactory factory;

    public InMemoryTypeManager() {
        factory = new StandardTypeFactory();
    }


    @Override
    public TitanKey createPropertyKey(InternalTitanTransaction tx, String name,
                                      TypeCategory category, Directionality directionality,
                                      TypeVisibility visibility, FunctionalType isfunctional, TitanType[] keysig, TitanType[] compactsig,
                                      TypeGroup group, boolean isUnique, IndexType[] indexes,
                                      Class<?> objectType) {
        StandardPropertyKey pt = new StandardPropertyKey(name, category, directionality, visibility,
                isfunctional, convertSignature(keysig), convertSignature(compactsig), group, isUnique, indexes, objectType);
        return factory.createNewPropertyKey(pt, tx);
    }

    @Override
    public TitanLabel createEdgeLabel(InternalTitanTransaction tx, String name,
                                      TypeCategory category, Directionality directionality,
                                      TypeVisibility visibility, FunctionalType isfunctional, TitanType[] keysig, TitanType[] compactsig,
                                      TypeGroup group) {
        StandardEdgeLabel rt = new StandardEdgeLabel(name, category, directionality, visibility,
                isfunctional, convertSignature(keysig), convertSignature(compactsig), group);
        return factory.createNewEdgeLabel(rt, tx);
    }

    @Override
    public TypeMaker getTypeMaker(InternalTitanTransaction tx) {
        return new StandardTypeMaker(tx, this);
    }

    @Override
    public InternalTitanType getType(long id, InternalTitanTransaction tx) {
        throw new UnsupportedOperationException("Not supported for InMemory Transactions");
    }

    @Override
    public InternalTitanType getType(String name, InternalTitanTransaction tx) {
        return null;
    }

    @Override
    public void committed(InternalTitanType type) {
        throw new UnsupportedOperationException("Not supported for InMemory Transactions");
    }

    @Override
    public boolean containsType(long id, InternalTitanTransaction tx) {
        return false;
    }

    @Override
    public boolean containsType(String name, InternalTitanTransaction tx) {
        return false;
    }

    @Override
    public void close() {
        //Nothing to do
    }

}
