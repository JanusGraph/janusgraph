package com.thinkaurelius.titan.net.msg.codec;

import com.thinkaurelius.titan.net.msg.Key;
import com.thinkaurelius.titan.net.msg.Reject;
import com.thinkaurelius.titan.net.msg.Reject.Code;
import edu.umd.umiacs.tcpcom.MiniBBInput;
import edu.umd.umiacs.tcpcom.MiniBBOutput;

import java.io.IOException;

public class RejectCodec implements Codec<Reject> {

	@Override
	public MiniBBOutput encode(Reject o, MiniBBOutput out) {
		Codecs.writeKey(o.getInstance(), out);
        if (o.getCode().equals(Code.BLACKLIST))
            out.writeByte((byte)0);
        else
            out.writeByte((byte)1);
        return out;
	}

	@Override
	public Reject decode(MiniBBInput in) throws IOException {
        Key key = Codecs.readKey(in);
        byte b = in.readByte();
        Code code = b == 0 ? Code.BLACKLIST : Code.BUSY;
        return new Reject(key, code);
	}

}
