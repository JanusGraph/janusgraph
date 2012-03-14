package com.thinkaurelius.titan.graphdb.edgetypes;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.Directionality;
import com.thinkaurelius.titan.core.EdgeCategory;
import com.thinkaurelius.titan.core.EdgeTypeGroup;
import com.thinkaurelius.titan.core.PropertyIndex;

public class StandardPropertyType extends AbstractEdgeTypeDefinition implements PropertyTypeDefinition {

	public PropertyIndex index;
	public boolean isKey;
	private Class<?> objectType;
	
	public StandardPropertyType() {}
	
	public StandardPropertyType(String name, EdgeCategory category,
			Directionality directionality, EdgeTypeVisibility visibility,
			boolean isfunctional, String[] keysig,
			String[] compactsig, EdgeTypeGroup group,
			boolean isKey, PropertyIndex index, Class<?> objectType) {
		super(name, category, directionality, visibility, isfunctional,
				keysig, compactsig, group);
		Preconditions.checkArgument(objectType!=null);
		this.index=index;
		this.isKey=isKey;
		this.objectType = objectType;
	}

	@Override
	public Class<?> getDataType() {
		return objectType;
	}

	@Override
	public PropertyIndex getIndexType() {
		return index;
	}

	@Override
	public boolean isKeyed() {
		return isKey;
	}

}
