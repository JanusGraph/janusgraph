package com.thinkaurelius.titan.net.msg.codec;

import com.thinkaurelius.titan.net.msg.Key;
import com.thinkaurelius.titan.net.msg.Query;
import com.thinkaurelius.titan.net.msg.Query.Mode;
import edu.umd.umiacs.tcpcom.MiniBBInput;
import edu.umd.umiacs.tcpcom.MiniBBOutput;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.EnumSet;

public class QueryCodec implements Codec<Query> {

	@Override
	public MiniBBOutput encode(Query o, MiniBBOutput out) {
		out.writeInt(o.getQueryType());
		out.writeInt(o.getGeneration());
		Codecs.writeKey(o.getSeed(), out);
		Codecs.writeKey(o.getInstance(), out);
		// Encode mode enum
        byte optionByte = 0;
        for (Mode m : o.getMode()) {
            optionByte |= (2 << m.ordinal());
        }
        out.writeByte(optionByte);
        if (null == o.getNodeId()) {
        	out.writeByte((byte)0);
        } else {
        	out.writeByte((byte)1);
        	out.writeLong(o.getNodeId());
        }
		out.copyBuffers(o.getData());
		return out;
	}

	@Override
	public Query decode(MiniBBInput in) throws IOException {
		int type = in.readInt();
		int gen  = in.readInt();
		Key seed = Codecs.readKey(in);
		Key tag  = Codecs.readKey(in);
		// Decode mode enum
		byte optionByte = in.readByte();
        EnumSet<Mode> modes = EnumSet.noneOf(Mode.class);
        for (Mode m : Mode.values()) {
            if (0 != (optionByte & (2 << m.ordinal())))
                modes.add(m);
        }
        byte nodeIdFlag = in.readByte();
        Long nodeId;
        if ((byte)1 == nodeIdFlag) {
        	nodeId = in.readLong();
        } else {
        	nodeId = null;
        }
        MiniBBOutput out = new MiniBBOutput();
        out.copyBuffers(in.getBuffers());
		ByteBuffer[] data = out.getBuffers();
		return new Query(seed, tag, gen, type, data, modes, nodeId);
	}
}
