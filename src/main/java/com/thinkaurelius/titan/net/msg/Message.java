package com.thinkaurelius.titan.net.msg;

import java.net.InetAddress;
import java.net.InetSocketAddress;


/**
 *
 * @author dalaro
 */
public abstract class Message {
	private InetAddress sender;
	private int replyPort;
	private Long receiptTime;
	
	public Message() {
		// all null
	}
	
	// Intended for outgoing message construction
	public Message(int replyPort) {
		this.replyPort = replyPort;
	}
	
	// Intended for incoming message construction
	public Message(InetAddress sender, int replyPort, Long receiptTime) {
		this.sender = sender;
		this.replyPort = replyPort;
		this.receiptTime = receiptTime;
	}
	
	public Long getReceiptTime() {
		return receiptTime;
	}
	
	public void setReceiptTime(Long receiptTime) {
		this.receiptTime = receiptTime;
	}

	public InetAddress getSender() {
		return sender;
	}

	public void setSender(InetAddress sender) {
		this.sender = sender;
	}

	public int getReplyPort() {
		return replyPort;
	}

	public void setReplyPort(int replyPort) {
		this.replyPort = replyPort;
	}
	
	/** Convenience method that puts together sender and replyPort */
	public InetSocketAddress getReplyAddress() {
		assert null != sender;
		return new InetSocketAddress(sender, replyPort);
	}
}
