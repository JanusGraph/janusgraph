package com.thinkaurelius.titan.net.msg.codec;

import com.thinkaurelius.titan.net.msg.Accept;
import edu.umd.umiacs.tcpcom.MiniBBInput;
import edu.umd.umiacs.tcpcom.MiniBBOutput;

import java.io.EOFException;
import java.net.UnknownHostException;

public class AcceptCodec implements Codec<Accept> {

	@Override
	public MiniBBOutput encode(Accept o, MiniBBOutput out) {
		Codecs.writeKey(o.getInstance(), out);
		return out;
	}

	@Override
	public Accept decode(MiniBBInput in) 
	throws UnknownHostException, EOFException {
		return new Accept(Codecs.readKey(in));
	}

}
