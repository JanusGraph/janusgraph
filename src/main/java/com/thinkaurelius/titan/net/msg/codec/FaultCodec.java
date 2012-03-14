package com.thinkaurelius.titan.net.msg.codec;

import com.thinkaurelius.titan.net.msg.Fault;
import com.thinkaurelius.titan.net.msg.Key;
import edu.umd.umiacs.tcpcom.MiniBBInput;
import edu.umd.umiacs.tcpcom.MiniBBOutput;

import java.io.IOException;

public class FaultCodec implements Codec<Fault> {

	@Override
	public MiniBBOutput encode(Fault o, MiniBBOutput out) {
		Codecs.writeKey(o.getSeed(), out);
		Codecs.writeString(o.getMessage(), out);
		return out;
	}

	@Override
	public Fault decode(MiniBBInput in) throws IOException {
		Key seed = Codecs.readKey(in);
		String message = Codecs.readString(in);
		return new Fault(seed, message);
	}

}
