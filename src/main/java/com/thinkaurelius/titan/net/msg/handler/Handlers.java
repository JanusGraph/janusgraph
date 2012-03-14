package com.thinkaurelius.titan.net.msg.handler;

import com.thinkaurelius.titan.net.Kernel;
import com.thinkaurelius.titan.net.msg.*;

import java.lang.reflect.Constructor;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Handlers {
	private Handlers() {}
	
	public static Map<Class<?>, Constructor<? extends Runnable>> getDefaultHandlerConstructors() throws SecurityException, NoSuchMethodException {
		Map<Class<?>, Constructor<? extends Runnable>> handlerConstructors =
			new ConcurrentHashMap<Class<?>, Constructor<? extends Runnable>>();
	    handlerConstructors.put(Accept.class, AcceptHandler.class.getConstructor(Kernel.class, Accept.class));
	    handlerConstructors.put(Fault.class, FaultHandler.class.getConstructor(Kernel.class, Fault.class));
	    handlerConstructors.put(Ping.class, PingHandler.class.getConstructor(Kernel.class, Ping.class));
	    handlerConstructors.put(Pong.class, PongHandler.class.getConstructor(Kernel.class, Pong.class));
	    handlerConstructors.put(Query.class, QueryHandler.class.getConstructor(Kernel.class, Query.class));
	    handlerConstructors.put(Reject.class, RejectHandler.class.getConstructor(Kernel.class, Reject.class));
	    handlerConstructors.put(Result.class, ResultHandler.class.getConstructor(Kernel.class, Result.class));
	    handlerConstructors.put(Trace.class, TraceHandler.class.getConstructor(Kernel.class, Trace.class));
	    return handlerConstructors;
	}

}
