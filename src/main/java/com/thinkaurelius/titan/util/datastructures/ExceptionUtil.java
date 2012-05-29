package com.thinkaurelius.titan.util.datastructures;

public class ExceptionUtil {

	public static final boolean isCausedBy(Throwable exception, Class<?> exType) {
		Throwable ex2 = exception.getCause();
		if (ex2==null || ex2 == exception) return false;
		else if (exType.isInstance(ex2)) return true;
		else return isCausedBy(ex2,exType);
	}
	
}
