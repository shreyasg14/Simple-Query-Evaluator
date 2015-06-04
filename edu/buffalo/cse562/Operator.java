package edu.buffalo.cse562;

import edu.buffalo.cse562.Datum;

public interface Operator {

	public Object[] readOneTuple();
	public void reset();
	public Datum[] getTuple();
	
}
