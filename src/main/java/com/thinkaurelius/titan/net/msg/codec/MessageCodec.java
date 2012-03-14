package com.thinkaurelius.titan.net.msg.codec;

import com.thinkaurelius.titan.net.msg.*;
import edu.umd.umiacs.tcpcom.MiniBBInput;
import edu.umd.umiacs.tcpcom.MiniBBOutput;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public enum MessageCodec {
	INSTANCE;
	
	private static byte typeCounter = -1;
    private static final Map<Byte, Codec<? extends Message>> codecs =
    	new ConcurrentHashMap<Byte, Codec<? extends Message>>();
    private static final Map<Class<?>, Byte> messageTypes =
    	new ConcurrentHashMap<Class<?>, Byte>();
    
    static {
    	registerMsgType(Accept.class, new AcceptCodec());
    	registerMsgType(Ping.class, new PingCodec());
    	registerMsgType(Pong.class, new PongCodec());
    	registerMsgType(Query.class, new QueryCodec());
    	registerMsgType(Reject.class, new RejectCodec());
    	registerMsgType(Result.class, new ResultCodec());
    	registerMsgType(Trace.class, new TraceCodec());
    }
    
    private static byte registerMsgType(Class<?> messageClass, Codec<? extends Message> messageCodec) {
    	typeCounter++;
    	codecs.put(typeCounter, messageCodec);
    	messageTypes.put(messageClass, typeCounter);
    	return typeCounter;
    }
    
    public static MessageCodec getDefaultCodec() {
    	return INSTANCE;
    }
    
    public Message decode(MiniBBInput in, ByteBuffer data[], InetSocketAddress sender) throws IOException {
        Byte type = in.readByte();
        int replyPort = in.readInt();
        Codec<? extends Message> codec = codecs.get(type);
        Message msg = codec.decode(in);
        msg.setReplyPort(replyPort);
        msg.setSender(sender.getAddress());
        return msg;
    }
    
	@SuppressWarnings("unchecked")
	public MiniBBOutput encode(Message m, MiniBBOutput out) {
    	Byte type = messageTypes.get(m.getClass());
    	out.writeByte(type);
    	out.writeInt(m.getReplyPort());
    	@SuppressWarnings("rawtypes")
		Codec codec = (Codec)codecs.get(type);
    	codec.encode(m, out);
    	return out;
    }
}
