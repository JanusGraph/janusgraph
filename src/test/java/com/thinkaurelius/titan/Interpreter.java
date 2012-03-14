package com.thinkaurelius.titan;


public class Interpreter {

	/**
	 * @param args
	 * @throws IllegalAccessException 
	 * @throws InstantiationException 
	 */
	public static void main(String[] args) throws InstantiationException, IllegalAccessException {
//		long out = 2l<<41l;
//		long in = 3;
//		System.out.println(out);
//		
//		B b = new B("This");
//		b.printHash();
//		b.print();
//		
//		C obj = C.class.newInstance();
//		System.out.println(obj.toString());


		int i = (-1000)%1000;
		System.out.println(i);
		
//		Object obj = "abc";
//		ResultCollector<?> res = null;
//		res.added(res.resultType().cast(obj));
		
	}


	
}

class H3 extends H2 {
	
	public H3(boolean h) {
		super(h);
	}
	
	public int hashCode() {
		return Object.class.cast(this).hashCode();
	}
	
}

class H2 extends H{
	
	public H2(boolean h) {
		super(h);
	}
	
	public int hashCode() {
		return 99;
	}
	
}

class H {
	
	boolean hash=true;
	public H(boolean h) {
		hash = h;
	}
	
	public int hashCode() {
		if (hash) {
			return 100;
		} else {
			return super.hashCode();
		}
	}
	
}

class C {
	
	public String toString() {
		return "This works";
	}
}


class A {
	
	protected Number obj;
	
	A(Number a) {
		obj=a;
	}
	
	public void printHash() {
		System.out.println(obj.hashCode());
	}
	
	
}

class B extends A{
	
	protected String obj;
	
	B(String b) {
		super(5.0);
		obj=b;
	}
	
	public void print() {
		System.out.println(obj);
	}
	
}