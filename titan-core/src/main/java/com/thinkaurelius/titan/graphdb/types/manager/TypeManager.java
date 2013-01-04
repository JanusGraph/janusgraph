package com.thinkaurelius.titan.graphdb.types.manager;


import com.thinkaurelius.titan.core.TitanKey;
import com.thinkaurelius.titan.core.TitanLabel;
import com.thinkaurelius.titan.core.TitanType;
import com.thinkaurelius.titan.core.TypeGroup;
import com.thinkaurelius.titan.core.TypeMaker;
import com.thinkaurelius.titan.graphdb.transaction.InternalTitanTransaction;
import com.thinkaurelius.titan.graphdb.types.Directionality;
import com.thinkaurelius.titan.graphdb.types.FunctionalType;
import com.thinkaurelius.titan.graphdb.types.InternalTitanType;
import com.thinkaurelius.titan.graphdb.types.TypeCategory;
import com.thinkaurelius.titan.graphdb.types.TypeVisibility;

public interface TypeManager {

    public TypeMaker getTypeMaker(InternalTitanTransaction tx);

    public TitanLabel createEdgeLabel(InternalTitanTransaction tx, String name, TypeCategory category,
                                      Directionality directionality, TypeVisibility visibility,
                                      FunctionalType isfunctional, TitanType[] keysig,
                                      TitanType[] compactsig, TypeGroup group);

    public TitanKey createPropertyKey(InternalTitanTransaction tx, String name, TypeCategory category,
                                      Directionality directionality, TypeVisibility visibility,
                                      FunctionalType isfunctional, TitanType[] keysig,
                                      TitanType[] compactsig, TypeGroup group,
                                      boolean isKey, boolean hasIndex, Class<?> objectType);


    public InternalTitanType getType(long id, InternalTitanTransaction tx);

    public InternalTitanType getType(String name, InternalTitanTransaction tx);

    public boolean containsType(long id, InternalTitanTransaction tx);

    public boolean containsType(String name, InternalTitanTransaction tx);

    public void committed(InternalTitanType type);

    public void close();


}
