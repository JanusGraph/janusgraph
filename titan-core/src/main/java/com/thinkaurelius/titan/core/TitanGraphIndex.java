package com.thinkaurelius.titan.core;

import com.tinkerpop.blueprints.Element;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface TitanGraphIndex extends TitanSchemaElement {

    public String getName();

    public String getBackingIndex();

    public Class<? extends Element> getIndexedElement();

    public TitanKey[] getFieldKeys();

    public Parameter[] getParametersFor(TitanKey key);

    public boolean isUnique();

}
