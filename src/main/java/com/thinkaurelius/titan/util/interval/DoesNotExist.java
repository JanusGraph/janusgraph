package com.thinkaurelius.titan.util.interval;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;

import java.util.Set;

/**
 * Represents a non-existence interval
 *
 */
public class DoesNotExist implements AtomicInterval {

    public static final AtomicInterval INSTANCE = new DoesNotExist();

	private DoesNotExist() {}
	
	@Override
	public boolean isPoint() {
		return true;
	}

	@Override
	public boolean isRange() {
		return false;
	}

	@Override
	public Object getEndPoint() {
		return null;
	}

	@Override
	public Object getStartPoint() {
		return null;
	}

	@Override
	public boolean endInclusive() {
		return true;
	}

	@Override
	public boolean startInclusive() {
		return true;
	}

    @Override
    public boolean hasHoles() {
        return false;
    }

    @Override
    public Set<Object> getHoles() {
        return ImmutableSet.of();
    }

	@Override
	public boolean inInterval(Object obj) {
		return false;
	}

    @Override
    public AtomicInterval intersect(AtomicInterval other) {
        if (other==this) return this;
        else return null;
    }

}
