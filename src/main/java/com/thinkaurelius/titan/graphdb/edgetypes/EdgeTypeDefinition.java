package com.thinkaurelius.titan.graphdb.edgetypes;

import com.thinkaurelius.titan.core.Directionality;
import com.thinkaurelius.titan.core.EdgeCategory;
import com.thinkaurelius.titan.core.EdgeType;
import com.thinkaurelius.titan.core.EdgeTypeGroup;


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
	
	public EdgeTypeGroup getGroup();

	public String[] getKeySignature();
	
	public String[] getCompactSignature();
	
	public boolean hasSignatureEdgeType(EdgeType et);
	
	public int getSignatureIndex(EdgeType et);

	
}
