package com.thinkaurelius.titan.graphdb.edgetypes;

import com.thinkaurelius.titan.core.Directionality;
import com.thinkaurelius.titan.core.EdgeCategory;
import com.thinkaurelius.titan.core.EdgeType;
import com.thinkaurelius.titan.core.EdgeTypeGroup;

import java.util.HashMap;
import java.util.Map;

public abstract class AbstractEdgeTypeDefinition implements EdgeTypeDefinition {

	public EdgeTypeVisibility visibility;
	public String name;
	public EdgeTypeGroup group;
	public boolean isfunctional;
	public Directionality directionality;
	public EdgeCategory category;
	public String[] keysig;
	public String[] compactsig;
	
	private transient Map<String,Integer> signatureIndex=null;
	
	//Needed for serialization
	AbstractEdgeTypeDefinition() {}
	
	AbstractEdgeTypeDefinition(String name, EdgeCategory category, Directionality directionality,
					EdgeTypeVisibility visibility, boolean isfunctional,
					String[] keysig, String[] compactsig, EdgeTypeGroup group) {
		this.name=name;
		this.group = group;
		this.category=category;
		this.directionality=directionality;
		this.visibility=visibility;
		this.isfunctional=isfunctional;
		this.keysig=keysig;
		this.compactsig=compactsig;
	}
	
	private Map<String,Integer> getSignatureIndex() {
		if (signatureIndex==null) {
			signatureIndex = new HashMap<String,Integer>();
			int pos = 0;
			for (String s : keysig) {
				signatureIndex.put(s, pos);
				pos++;
			}
			for (String s : compactsig) {
				signatureIndex.put(s, pos);
				pos++;
			}
		}
		return signatureIndex;
	}
	
	public boolean hasSignatureEdgeType(EdgeType et) {
		return getSignatureIndex().containsKey(et.getName());		
	}
	
	public int getSignatureIndex(EdgeType et) {
		Integer i = getSignatureIndex().get(et.getName());
		if (i==null)
			throw new IllegalArgumentException("The provided EdgeType is not part of the signature: " + et);
		return i;
	}
	
	
	@Override
	public String[] getCompactSignature() {
		return compactsig;
	}

	@Override
	public String[] getKeySignature() {
		return keysig;
	}


	@Override
	public EdgeCategory getCategory() {
		return category;
	}

	@Override
	public Directionality getDirectionality() {
		return directionality;
	}

	@Override
	public String getName() {
		return name;
	}
	
	@Override
	public EdgeTypeGroup getGroup() {
		return group;
	}


	@Override
	public boolean isFunctional() {
		return isfunctional;
	}

	@Override
	public boolean isModifiable() {
		return visibility.isModifiable();
	}
	

	@Override
	public boolean isHidden() {
		return visibility.isHidden();
	}

	
}
