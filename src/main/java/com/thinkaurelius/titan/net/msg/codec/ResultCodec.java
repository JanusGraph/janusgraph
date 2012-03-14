package com.thinkaurelius.titan.net.msg.codec;

import com.thinkaurelius.titan.net.msg.Key;
import com.thinkaurelius.titan.net.msg.Result;
import edu.umd.umiacs.tcpcom.MiniBBInput;
import edu.umd.umiacs.tcpcom.MiniBBOutput;

import java.io.IOException;
import java.nio.ByteBuffer;

public class ResultCodec implements Codec<Result> {

	@Override
	public MiniBBOutput encode(Result o, MiniBBOutput out) {
		Codecs.writeKey(o.getSeed(), out);
		out.includeBuffers(o.getData());
		return out;
	}

	@Override
	public Result decode(MiniBBInput in) throws IOException {
		Key seed = Codecs.readKey(in);
		ByteBuffer[] data = in.getBuffers();
		return new Result(seed, data);
	}

}
