package com.thinkaurelius.titan.net.msg;

/**
 *
 * @author dalaro
 */
public class Trace extends Message {
    private final Key seed;
    private final Key instance;
    private final Code code;

    public static enum Code { BEGIN, END, OK };
    
    public Trace(Key seed, Key instance, Code code) {
        this.seed = seed;
        this.instance = instance;
        this.code = code;
    }

    public Code getCode() {
        return code;
    }

    public Key getSeed() {
        return seed;
    }

    public Key getInstance() {
        return instance;
    }
    
	@Override
	public String toString() {
		return "Trace[seed=" + seed + ", instance=" + instance + ", code="
				+ code + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((code == null) ? 0 : code.hashCode());
		result = prime * result
				+ ((instance == null) ? 0 : instance.hashCode());
		result = prime * result + ((seed == null) ? 0 : seed.hashCode());
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
		Trace other = (Trace) obj;
		if (code != other.code)
			return false;
		if (instance == null) {
			if (other.instance != null)
				return false;
		} else if (!instance.equals(other.instance))
			return false;
		if (seed == null) {
			if (other.seed != null)
				return false;
		} else if (!seed.equals(other.seed))
			return false;
		return true;
	}
    
    
}
