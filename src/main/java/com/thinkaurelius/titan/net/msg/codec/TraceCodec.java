package com.thinkaurelius.titan.net.msg.codec;

import com.thinkaurelius.titan.net.msg.Key;
import com.thinkaurelius.titan.net.msg.Trace;
import com.thinkaurelius.titan.net.msg.Trace.Code;
import edu.umd.umiacs.tcpcom.MiniBBInput;
import edu.umd.umiacs.tcpcom.MiniBBOutput;

import java.io.IOException;

public class TraceCodec implements Codec<Trace> {

	@Override
	public MiniBBOutput encode(Trace o, MiniBBOutput out) {
		Codecs.writeKey(o.getSeed(), out);
		Codecs.writeKey(o.getInstance(), out);
		out.writeInt(o.getCode().ordinal());
		return out;
	}

	@Override
	public Trace decode(MiniBBInput in) throws IOException {
		Key seed = Codecs.readKey(in);
		Key instance = Codecs.readKey(in);
		Code code = Code.values()[in.readInt()];
		return new Trace(seed, instance, code);
	}

}
