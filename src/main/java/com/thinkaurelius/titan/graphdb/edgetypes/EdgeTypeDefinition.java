package com.thinkaurelius.titan.graphdb.edgetypes;

import com.thinkaurelius.titan.core.TitanType;
import com.thinkaurelius.titan.core.TypeGroup;


public interface EdgeTypeDefinition {
	
	/**
	 * Returns the name of this association type. Names must be unique across all association types.
	 * @return Name of this association.
	 */
	public String getName();
	
	public boolean isFunctional();
	
	public Directionality getDirectionality();	
	
	public boolean isHidden();
	
	public boolean isModifiable();
	
	public EdgeCategory getCategory();
	
	public TypeGroup getGroup();

	public String[] getKeySignature();
	
	public String[] getCompactSignature();

	
}
