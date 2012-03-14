package com.thinkaurelius.titan.graphdb.edgetypes;

import com.thinkaurelius.titan.core.EdgeType;
import com.thinkaurelius.titan.core.EdgeTypeGroup;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Comparator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class EdgeTypeComparator implements Comparator<EdgeType> {

	public static final EdgeTypeComparator Instance = new EdgeTypeComparator();
	
	private EdgeTypeComparator() {
		
	}
	
	@Override
	public int compare(EdgeType et1, EdgeType et2) {
		EdgeTypeGroup g1 = et1.getGroup(), g2 = et2.getGroup();
		if (g1.getID()==g2.getID()) {
			return et1.getName().compareTo(et2.getName());
		} else return g1.getID()-g2.getID();
	}
	
	
	
	
	private static final ConcurrentMap<Short,EdgeType> groupEdgeTypes = new ConcurrentHashMap<Short,EdgeType>();

	private static Method getID=null;
	private static Method getName=null;
	
	public static final EdgeType getGroupComparisonEdgeType(final short id) {
		if (getID==null || getName==null) {
			try {
				getID=EdgeType.class.getMethod("getID");
				getName=EdgeType.class.getMethod("getName");
			} catch (NoSuchMethodException e) {
				throw new AssertionError("Invalid method references!");
			}
		}
		
		EdgeType et = groupEdgeTypes.get(Short.valueOf(id));
		if (et==null) {
			
			InvocationHandler handler = new InvocationHandler() {
				
				@Override
				public Object invoke(Object proxy, Method method, Object[] args)
						throws Throwable {
					if (method.equals(getID)) {
						return id;
					} else if (method.equals(getName)) {
						return "";
					} else throw new UnsupportedOperationException("Not supported on comparison edge type");
				}
				
			};
			et = (EdgeType)Proxy.newProxyInstance(EdgeType.class.getClassLoader(), new Class[] {EdgeType.class}, handler);
			groupEdgeTypes.putIfAbsent(Short.valueOf(id), et);
		}
		return et;
	}

}
