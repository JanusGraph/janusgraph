package com.thinkaurelius.titan.graphdb.types;

import com.thinkaurelius.titan.core.TypeGroup;


public interface TypeDefinition {
	
	/**
	 * Returns the name of this association type. Names must be unique across all association types.
	 * @return Name of this association.
	 */
	public String getName();
	
	public boolean isFunctional();

    public boolean isFunctionalLocking();
	
	public Directionality getDirectionality();	
	
	public boolean isHidden();
	
	public boolean isModifiable();
	
	public TypeCategory getCategory();
	
	public TypeGroup getGroup();

	public String[] getKeySignature();
	
	public String[] getCompactSignature();

	
}
