package com.thinkaurelius.titan.graphdb.types.manager;


import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.graphdb.types.*;
import com.thinkaurelius.titan.graphdb.transaction.InternalTitanTransaction;

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
