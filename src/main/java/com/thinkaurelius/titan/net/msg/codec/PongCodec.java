package com.thinkaurelius.titan.net.msg.codec;

import com.thinkaurelius.titan.net.msg.Key;
import com.thinkaurelius.titan.net.msg.Pong;
import edu.umd.umiacs.tcpcom.MiniBBInput;
import edu.umd.umiacs.tcpcom.MiniBBOutput;

import java.io.IOException;
import java.util.Collection;

public class PongCodec implements Codec<Pong> {

	@Override
	public MiniBBOutput encode(Pong o, MiniBBOutput out) {
		Codecs.writeKey(o.getKey(), out);
		Codecs.writeLongs(o.getQueryKeyIds(), out);
		return out;
	}

	@Override
	public Pong decode(MiniBBInput in) throws IOException {
		Key seed = Codecs.readKey(in);
		Collection<Long> ids = Codecs.readLongs(in);
		return new Pong(seed, ids);
	}
}
