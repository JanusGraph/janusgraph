package com.thinkaurelius.titan.graphdb.adjacencylist;

import com.thinkaurelius.titan.core.TitanType;
import com.thinkaurelius.titan.core.TypeGroup;

import java.util.Comparator;

public class TypeComparator implements Comparator<TitanType> {

    public static final TypeComparator INSTANCE = new TypeComparator();

    private TypeComparator() {

    }

    @Override
    public int compare(TitanType et1, TitanType et2) {
        TypeGroup g1 = et1.getGroup(), g2 = et2.getGroup();
        if (g1.equals(g2)) {
            return et1.getName().compareTo(et2.getName());
        } else return compareGroups(g1, g2);
    }

    public int compareGroups(TypeGroup g1, TypeGroup g2) {
        return g1.getID() - g2.getID();
    }


//	private static final ConcurrentMap<Short,TitanType> groupTypes = new ConcurrentHashMap<Short,TitanType>();
//
//	private volatile static Method getID=null;
//	private volatile static Method getName=null;
//
//	public static final TitanType getGroupComparisonType(final short id) {
//		if (getID==null || getName==null) {
//			try {
//				getID=TitanType.class.getMethod("getID");
//				getName=TitanType.class.getMethod("getName");
//			} catch (NoSuchMethodException e) {
//				throw new AssertionError("Invalid method references");
//			}
//		}
//
//		TitanType et = groupTypes.get(Short.valueOf(id));
//		if (et==null) {
//
//			InvocationHandler handler = new InvocationHandler() {
//
//				@Override
//				public Object invoke(Object proxy, Method method, Object[] args)
//						throws Throwable {
//					if (method.equals(getID)) {
//						return id;
//					} else if (method.equals(getName)) {
//						return "";
//					} else throw new UnsupportedOperationException("Not supported on comparison edge type");
//				}
//
//			};
//			et = (TitanType)Proxy.newProxyInstance(TitanType.class.getClassLoader(), new Class[] {TitanType.class}, handler);
//            groupTypes.putIfAbsent(Short.valueOf(id), et);
//		}
//		return et;
//	}

}
