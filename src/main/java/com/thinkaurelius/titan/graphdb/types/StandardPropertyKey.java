package com.thinkaurelius.titan.graphdb.types;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.TypeGroup;

public class StandardPropertyKey extends AbstractTypeDefinition implements PropertyKeyDefinition {

	public boolean hasIndex;
	public boolean isKey;
	private Class<?> objectType;
	
	public StandardPropertyKey() {}
	
	public StandardPropertyKey(String name, TypeCategory category,
                               Directionality directionality, TypeVisibility visibility,
                               FunctionalType isfunctional, String[] keysig,
                               String[] compactsig, TypeGroup group,
                               boolean isKey, boolean hasIndex, Class<?> objectType) {
		super(name, category, directionality, visibility, isfunctional,
				keysig, compactsig, group);
		Preconditions.checkArgument(objectType!=null);
		this.hasIndex = hasIndex;
		this.isKey=isKey;
		this.objectType = objectType;
	}

	@Override
	public Class<?> getDataType() {
		return objectType;
	}

	@Override
	public boolean hasIndex() {
		return hasIndex;
	}

	@Override
	public boolean isUnique() {
		return isKey;
	}

}
