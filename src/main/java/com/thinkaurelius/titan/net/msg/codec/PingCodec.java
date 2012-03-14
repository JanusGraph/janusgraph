package com.thinkaurelius.titan.net.msg.codec;

import com.thinkaurelius.titan.net.msg.Ping;
import edu.umd.umiacs.tcpcom.MiniBBInput;
import edu.umd.umiacs.tcpcom.MiniBBOutput;

import java.io.IOException;

public class PingCodec implements Codec<Ping> {

	@Override
	public MiniBBOutput encode(Ping o, MiniBBOutput out) {
		Codecs.writeKey(o.getClient(), out);
		return out;
	}

	@Override
	public Ping decode(MiniBBInput in) throws IOException {
		return new Ping(Codecs.readKey(in));
	}

}
