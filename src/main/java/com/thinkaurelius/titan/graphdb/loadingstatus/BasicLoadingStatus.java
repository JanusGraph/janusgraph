package com.thinkaurelius.titan.graphdb.loadingstatus;

import cern.colt.map.AbstractIntIntMap;
import cern.colt.map.OpenIntIntHashMap;
import com.thinkaurelius.titan.core.TitanType;
import com.thinkaurelius.titan.graphdb.query.QueryUtil;
import com.thinkaurelius.titan.graphdb.query.InternalTitanQuery;

import java.util.HashMap;
import java.util.Map;

public class BasicLoadingStatus implements LoadingStatus {
	
	private Map<TitanType,Byte> loadedTypes;
	private byte allLoadedDirsIndex;
	private AbstractIntIntMap groups;
	
	
	BasicLoadingStatus() {
		allLoadedDirsIndex = 0;
		loadedTypes = null;
		groups = null;
	}

	
	/* ---------------------------------------------------------------
	 * Loaded TitanRelation Maintenance
	 * ---------------------------------------------------------------
	 */

	@Override
	public boolean hasLoadedEdges(InternalTitanQuery query) {
		if (DirectionTypeEncoder.hasAllCovered(allLoadedDirsIndex, query)) return true;
		
		if (groups!=null && (query.hasGroupCondition() || query.hasEdgeTypeCondition())) {
			short groupid=-1;
			if (query.hasGroupCondition()) groupid = query.getGroupCondition().getID();
			else if (query.hasEdgeTypeCondition()) groupid = query.getTypeCondition().getGroup().getID();
			assert groupid>=0;
			
			if (DirectionTypeEncoder.hasAllCovered((byte)groups.get(groupid), query)) return true;
		}
		
		if (loadedTypes!=null && query.hasEdgeTypeCondition()) {			
			TitanType type = query.getTypeCondition();
			Byte code = loadedTypes.get(type);
			if (code!=null && DirectionTypeEncoder.hasAllCovered(code.byteValue(), query)) return true;
		}
		return false;
	}


	
	@Override
	public LoadingStatus loadedEdges(InternalTitanQuery query) {
		if (query.hasEdgeTypeCondition()) {
            if (!QueryUtil.hasFirstKeyConstraint(query)) {
                TitanType type = query.getTypeCondition();
                if (loadedTypes==null) loadedTypes = new HashMap<TitanType,Byte>();
                byte code = 0;
                Byte tmp = loadedTypes.get(type);
                if (tmp!=null) code = tmp.byteValue();
                code = DirectionTypeEncoder.loaded(code, query);
                loadedTypes.put(type, Byte.valueOf(code));
            }
		} else if (query.hasGroupCondition()) {
			short groupid = query.getGroupCondition().getID();
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
