package com.thinkaurelius.titan.net.msg;

/**
 *
 * @author dalaro
 */
public class Reject extends Message {

    public static enum Code { BLACKLIST, BUSY };

    public final Key instance;
    public final Code code;

    public Reject(Key instance, Code code) {
        this.instance = instance;
        this.code = code;
    }

    public Code getCode() {
        return code;
    }

    public Key getInstance() {
        return instance;
    }

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((code == null) ? 0 : code.hashCode());
		result = prime * result
				+ ((instance == null) ? 0 : instance.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Reject other = (Reject) obj;
		if (code != other.code)
			return false;
		if (instance == null) {
			if (other.instance != null)
				return false;
		} else if (!instance.equals(other.instance))
			return false;
		return true;
	}
    
    
}
