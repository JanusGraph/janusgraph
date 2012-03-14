package com.thinkaurelius.titan.net.msg.codec;

import edu.umd.umiacs.tcpcom.MiniBBInput;
import edu.umd.umiacs.tcpcom.MiniBBOutput;

import java.io.IOException;

public interface Codec<T> {
	public MiniBBOutput encode(T o, MiniBBOutput out);
	public T decode(MiniBBInput in) throws IOException;
		
	public static enum tid {
		ACCEPT,
		FAULT,
		PING,
		PONG,
		QUERY,
		REJECT,
		RESULT,
		TRACE
	};
}
