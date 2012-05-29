package com.thinkaurelius.titan.graphdb.types;

import com.thinkaurelius.titan.core.TitanType;
import com.thinkaurelius.titan.core.TypeGroup;

import java.util.HashMap;
import java.util.Map;

public abstract class AbstractTypeDefinition implements TypeDefinition {

	public TypeVisibility visibility;
	public String name;
	public TypeGroup group;
	public FunctionalType isfunctional;
	public Directionality directionality;
	public TypeCategory category;
	public String[] keysig;
	public String[] compactsig;
	
	private transient Map<String,Integer> signatureIndex=null;
	
	//Needed for serialization
	AbstractTypeDefinition() {}
	
	AbstractTypeDefinition(String name, TypeCategory category, Directionality directionality,
                           TypeVisibility visibility, FunctionalType isfunctional,
                           String[] keysig, String[] compactsig, TypeGroup group) {
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
	
	public boolean hasSignatureEdgeType(TitanType et) {
		return getSignatureIndex().containsKey(et.getName());		
	}
	
	public int getSignatureIndex(TitanType et) {
		Integer i = getSignatureIndex().get(et.getName());
		if (i==null)
			throw new IllegalArgumentException("The provided TitanType is not part of the signature: " + et);
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
	public TypeCategory getCategory() {
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
	public TypeGroup getGroup() {
		return group;
	}

	@Override
	public boolean isFunctional() {
		return isfunctional.isFunctional();
	}

    @Override
    public boolean isFunctionalLocking() {
        return isfunctional.isLocking();
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
