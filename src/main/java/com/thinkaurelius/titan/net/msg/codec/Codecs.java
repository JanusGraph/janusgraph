package com.thinkaurelius.titan.net.msg.codec;

import com.thinkaurelius.titan.net.msg.Key;
import edu.umd.umiacs.tcpcom.MiniBBInput;
import edu.umd.umiacs.tcpcom.MiniBBOutput;

import java.io.EOFException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;

/**
 * Utility methods used by the Codec implementations.
 * 
 * @author dalaro
 */
public class Codecs {
	
	private Codecs() {}
	
	static void writeInetSocketAddress(
			InetSocketAddress addr, MiniBBOutput out) {
        byte[] ip = addr.getAddress().getAddress();
        assert 4 == ip.length;
        out.write(ip);
        out.writeInt(addr.getPort());
	}
	
	static InetSocketAddress readInetSocketAddress(MiniBBInput in) 
	throws UnknownHostException, EOFException {
        byte[] ip = new byte[4];
        in.readFully(ip);
        int port = in.readInt();
        return new InetSocketAddress(InetAddress.getByAddress(ip), port);
	}
	
    static void writeKey(Key k, MiniBBOutput out) {
        out.writeLong(k.getId());
        writeInetSocketAddress(k.getHost(), out);
        out.writeLong(k.getHostBootTime());
    }

    static Key readKey(MiniBBInput in) 
    throws EOFException, UnknownHostException {
    	long id = in.readLong();
    	InetSocketAddress host = readInetSocketAddress(in);
    	long bootTime = in.readLong();
        return new Key(id, host, bootTime);
    }
        
    static void writeString(String s, MiniBBOutput out) {
        if (null != s) {
            byte[] buf = s.getBytes();
            out.writeInt(buf.length);
            out.write(buf);
        } else {
            out.writeInt(-1);
        }
    }

    static String readString(MiniBBInput in) throws EOFException {
        int messageLength = in.readInt();
        if (-1 == messageLength)
        	return null;
        if (0 == messageLength)
        	return "";
        byte[] messageBuf = new byte[messageLength];
        in.readFully(messageBuf);
        return new String(messageBuf);
    }
    
    static void writeLongs(Collection<Long> c, MiniBBOutput out) {
    	out.writeInt(c.size());
    	for (Long l : c)
    		out.writeLong(l);
    }
    
    static Collection<Long> readLongs(MiniBBInput in) throws EOFException {
    	int size = in.readInt();
    	Collection<Long> result = new ArrayList<Long>(size);
    	for (int i = 0; i < size; i++)
    		result.add(in.readLong());
    	return result;
    }
}
