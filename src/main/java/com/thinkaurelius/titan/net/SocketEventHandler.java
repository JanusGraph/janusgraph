package com.thinkaurelius.titan.net;

import com.thinkaurelius.titan.net.msg.Message;
import com.thinkaurelius.titan.net.msg.codec.MessageCodec;
import com.thinkaurelius.titan.net.msg.handler.Handlers;
import edu.umd.umiacs.tcpcom.EventHandler;
import edu.umd.umiacs.tcpcom.MiniBBInput;
import edu.umd.umiacs.tcpcom.SocketCommunicator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Map;

class SocketEventHandler implements EventHandler {

	private final Kernel kernel;

	SocketEventHandler(Kernel kernel) {
		this.kernel = kernel;
	}

	@Override
	public void connectionAccepted(InetSocketAddress peer,
			SocketCommunicator com) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void connectionFinished(InetSocketAddress peer,
			SocketCommunicator com) {
		// TODO Auto-generated method stub
	}

	@Override
	public void disconnected(InetSocketAddress peer, IOException reason,
			SocketCommunicator com) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void rxData(InetSocketAddress sender, ByteBuffer[] data,
			SocketCommunicator com) {
		Runnable r = new RxDataDecoderTask(data, sender, kernel);
		kernel.executeNow(r);
	}
	
	private static class RxDataDecoderTask implements Runnable {
		private final ByteBuffer data[];
		private final InetSocketAddress sender;
		private final Kernel kernel;
		private static final MessageCodec codec = MessageCodec.getDefaultCodec();
		private static final Map<Class<?>, Constructor<? extends Runnable>> handlerCtors;
		private static final Logger logger =
			LoggerFactory.getLogger(RxDataDecoderTask.class);
		
		static {
			try {
		        // Get constructors
				handlerCtors = Handlers.getDefaultHandlerConstructors();
			} catch (NoSuchMethodException e) {
				throw new RuntimeException(e);
			}
		}
		
		private RxDataDecoderTask(ByteBuffer[] data, InetSocketAddress sender,
				Kernel kernel) {
			this.data = data;
			this.sender = sender;
			this.kernel = kernel;
		}

		@Override
		public void run() {
			MiniBBInput in = new MiniBBInput(data);
			
			Message msg = null;
			try {
				msg = codec.decode(in, data, sender);
            } catch (IOException e) {
                logger.error("Message deserialization failure", e);
                return;
            }

			boolean ok = false;
			try {
//				logger.debug("Attempting to construct Runnable to process this message");
				Class<?> c = msg.getClass();
				Constructor<? extends Runnable> hc = handlerCtors.get(c);
				kernel.executeNow(hc.newInstance(kernel, msg));
				ok = true;
//				logger.debug("Runnable constructed and enqueued for processing");
			} catch (IllegalArgumentException e) {
				logger.error("Handler construction failure", e);
			} catch (InstantiationException e) {
				logger.error("Handler construction failure", e);
			} catch (IllegalAccessException e) {
				logger.error(
						"Handler construction failure (likely insufficient access)",
						e);
			} catch (InvocationTargetException e) {
				logger.error(
						"Handler construction failure (likely wrong constructor signature)",
						e);
				logger.error("InvocationTargetException.getTargetException()",
						e.getTargetException());
			} finally {
				if (!ok) {
					logger.error("Dropping message from " + sender);
				}
			}
		}
	}
}
