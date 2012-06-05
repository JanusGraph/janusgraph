package com.thinkaurelius.titan.graphdb.database.statistics;

import com.thinkaurelius.titan.core.GraphDatabaseException;
import com.thinkaurelius.titan.core.TitanType;
import com.thinkaurelius.titan.core.TypeGroup;
import com.thinkaurelius.titan.util.datastructures.LongCounter;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class LocalGraphStatistics implements InternalGraphStatistics {

	private static final String Separator = ".";
	private static final String etName = "edgetype";
	private static final String groupName = "group";
	
	private final PropertiesConfiguration persistence;
	
	private final Map<String,LongCounter> edgeTypes;
	private final Map<Integer,LongCounter> groups;
	private long noNodes;
	private long noEdges;
	private long noEdgeTypes;
	private long noRelationships;
	private long noProperties;
	
	public LocalGraphStatistics(PropertiesConfiguration file) {
		persistence=file;
		//Initialize
		noNodes = getCount("numNodes");
		noEdgeTypes = getCount("numEdgeTypes");
		noEdges = getCount("numEdges");
		noRelationships = getCount("numRelationship");
		noProperties = getCount("numProperties");
		edgeTypes = new ConcurrentHashMap<String,LongCounter>();
		groups = new ConcurrentHashMap<Integer,LongCounter>();
		Iterator<String> ets = persistence.getKeys(etName+Separator);
		while (ets.hasNext()) {
			String key = ets.next();
			edgeTypes.put(getID(key), new LongCounter(persistence.getLong(key)));
		}
		Iterator<String> groupiter = persistence.getKeys(groupName+Separator);
		while (groupiter.hasNext()) {
			String key = groupiter.next();
			groups.put(Integer.parseInt(getID(key)), new LongCounter(persistence.getLong(key)));
		}
	}
	
	private void save() {
		setCount("numNodes",noNodes);
		setCount("numEdgeTypes",noEdgeTypes);
		setCount("numEdges",noEdges);
		setCount("numRelationship",noRelationships);
		setCount("numProperties",noProperties);
		for (Map.Entry<String, LongCounter> entry : edgeTypes.entrySet()) {
			setCount(etName,entry.getKey(),entry.getValue().get());
		}
		for (Map.Entry<Integer, LongCounter> entry : groups.entrySet()) {
			setCount(groupName,entry.getKey().toString(),entry.getValue().get());
		}
		try {
			persistence.save();
		} catch (ConfigurationException e) {
			throw new GraphDatabaseException("Could not save statistics",e);
		}
	}
	
	private String getID(String handle) {
		return handle.substring(handle.lastIndexOf(Separator)+1);
	}
	
	private String getHandle(String type, String id) {
		return (type==null?id:type+Separator+id);
	}
	
	private long getCount(String id) {
		return getCount(null,id);
	}
	
	private long getCount(String type, String id) {
		return persistence.getLong(getHandle(type,id), 0);
	}
	
	private void setCount(String id, long value) {
		setCount(null,id,value);
	}
	
	private void setCount(String type, String id, long value) {
		persistence.setProperty(getHandle(type,id), value);
	}
	
	
	@Override
	public synchronized void update(TransactionStatistics stats) {
		noNodes += stats.getNodeDelta();
		noEdgeTypes += stats.getEdgeTypeDelta();
		for (Map.Entry<TitanType, LongCounter> entry : stats.getDeltaEdgeTypes().entrySet()) {
			LongCounter lc = entry.getValue();
			TitanType type = entry.getKey();
			
			LongCounter count = edgeTypes.get(type.getName());
			if (count==null) {
				count = new LongCounter(0);
				edgeTypes.put(type.getName(), count);
			}
			count.increment(lc.get());
			
			count = groups.get((int)type.getGroup().getID());
			if (count==null) {
				count = new LongCounter(0);
				groups.put((int)type.getGroup().getID(), count);
			}
			count.increment(lc.get());
			
			noEdges += lc.get();
			if (type.isPropertyKey()) {
				noProperties += lc.get();
			} else {
				assert type.isEdgeLabel();
				noRelationships += lc.get();
			}
		}
		assert noNodes>=0;
		assert noEdges>=0;
		assert noRelationships>=0;
		assert noProperties>=0;
		save();
	}


	@Override
	public long getNoEdges() {
		return noEdges;
	}

	@Override
	public long getNoEdges(String edgeTypeName) {
		LongCounter lc = edgeTypes.get(edgeTypeName);
		if (lc==null) return 0;
		else return lc.get();
	}

	@Override
	public long getNoEdges(TypeGroup group) {
		LongCounter lc = groups.get(group.getID());
		if (lc==null) return 0;
		else return lc.get();	
	}

	@Override
	public long getNoNodes() {
		return noNodes;
	}
	
	@Override
	public long getNoEdgeTypes() {
		return noEdgeTypes;
	}

	@Override
	public long getNoProperties() {
		return noProperties;
	}

	@Override
	public long getNoRelationships() {
		return noRelationships;
	}

	
	
}
