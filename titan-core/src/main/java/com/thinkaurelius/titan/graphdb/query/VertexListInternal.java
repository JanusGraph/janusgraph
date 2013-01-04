package com.thinkaurelius.titan.graphdb.query;

import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.core.VertexList;

public interface VertexListInternal extends VertexList {

    public void add(TitanVertex n);

    public void addAll(VertexList vertices);

}
