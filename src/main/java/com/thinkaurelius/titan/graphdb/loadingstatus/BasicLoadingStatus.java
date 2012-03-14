package com.thinkaurelius.titan.graphdb.loadingstatus;

import cern.colt.map.AbstractIntIntMap;
import cern.colt.map.OpenIntIntHashMap;
import com.thinkaurelius.titan.core.EdgeType;
import com.thinkaurelius.titan.graphdb.edgequery.InternalEdgeQuery;

import java.util.HashMap;
import java.util.Map;

public class BasicLoadingStatus implements LoadingStatus {
	
	private Map<EdgeType,Byte> loadedTypes;
	private byte allLoadedDirsIndex;
	private AbstractIntIntMap groups;
	
	
	BasicLoadingStatus() {
		allLoadedDirsIndex = 0;
		loadedTypes = null;
		groups = null;
	}

	
	/* ---------------------------------------------------------------
	 * Loaded Edge Maintenance
	 * ---------------------------------------------------------------
	 */

	@Override
	public boolean hasLoadedEdges(InternalEdgeQuery query) {
		if (DirectionTypeEncoder.hasAllCovered(allLoadedDirsIndex, query)) return true;
		
		if (groups!=null && (query.hasEdgeTypeGroupCondition() || query.hasEdgeTypeCondition())) {
			short groupid=-1;
			if (query.hasEdgeTypeGroupCondition()) groupid = query.getEdgeTypeGroupCondition().getID();
			else if (query.hasEdgeTypeCondition()) groupid = query.getEdgeTypeCondition().getGroup().getID();
			assert groupid>=0;
			
			if (DirectionTypeEncoder.hasAllCovered((byte)groups.get(groupid), query)) return true;
		}
		
		if (loadedTypes!=null && query.hasEdgeTypeCondition()) {			
			EdgeType type = query.getEdgeTypeCondition();
			Byte code = loadedTypes.get(type);
			if (code!=null && DirectionTypeEncoder.hasAllCovered(code.byteValue(), query)) return true;
		}
		return false;
	}


	
	@Override
	public LoadingStatus loadedEdges(InternalEdgeQuery query) {
		if (query.hasEdgeTypeCondition()) {
			EdgeType type = query.getEdgeTypeCondition();
			if (loadedTypes==null) loadedTypes = new HashMap<EdgeType,Byte>();
			byte code = 0;
			Byte tmp = loadedTypes.get(type);
			if (tmp!=null) code = tmp.byteValue();
			code = DirectionTypeEncoder.loaded(code, query);
			loadedTypes.put(type, Byte.valueOf(code));
		} else if (query.hasEdgeTypeGroupCondition()) {
			short groupid = query.getEdgeTypeGroupCondition().getID();
			if (groups==null) groups=new OpenIntIntHashMap(5);
			byte code = (byte)groups.get(groupid);
			code = DirectionTypeEncoder.loaded(code, query);
			groups.put(groupid, code);
		} else {
			allLoadedDirsIndex=DirectionTypeEncoder.loaded(allLoadedDirsIndex,query);
		}
		return this;
	}

	
}
