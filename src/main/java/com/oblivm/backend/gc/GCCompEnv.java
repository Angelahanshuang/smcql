package com.oblivm.backend.gc;

import com.oblivm.backend.flexsc.CompEnv;
import com.oblivm.backend.flexsc.Mode;
import com.oblivm.backend.flexsc.Party;
import com.oblivm.backend.network.Network;

public abstract class GCCompEnv extends CompEnv<GCSignal> {
	/**
	 * 
	 */
	private static final long serialVersionUID = -1959218203163444929L;

	public GCCompEnv(Network channel, Party p, Mode mode) {
		super(channel, p, mode);
	}

	public GCSignal ONE() {
		return new GCSignal(true);
	}
	
	public GCSignal ZERO() {
		return new GCSignal(false);
	}
	
	public GCSignal[] newTArray(int len) {
		return new GCSignal[len];
	}
	
	public GCSignal[][] newTArray(int d1, int d2) {
		return new GCSignal[d1][d2];
	}
	
	public GCSignal[][][] newTArray(int d1, int d2, int d3) {
		return new GCSignal[d1][d2][d3];
	}
	
	public GCSignal newT(boolean v) {
		return new GCSignal(v);
	}

	public void print(String msg, GCSignal in){
		System.out.println(msg + (in.isPublic() ? in.v : "value"));
	}

	public boolean compare(GCSignal a, GCSignal b){
		if (a.isPublic() && b.isPublic())
			return a.v == b.v;
		else if (a.isPublic())
			return false;
		else if (b.isPublic())
			return false;
		else {
			if(a.bytes.length != b.bytes.length)
				return false;
			boolean eq = true;
			for(int i = 0; i < a.bytes.length; i++){
				if(a.bytes[i] != b.bytes[i])
					eq = false;
			}
			return eq;
		}
	}
	public boolean compare(GCSignal[] a, GCSignal[] b){
		if(a.length != b.length)
			return false;
		boolean eq = true;
		for(int i = 0; i < a.length; i++){
			if(a[i] != b[i])
				eq = false;
		}
		return eq;
	}
}
