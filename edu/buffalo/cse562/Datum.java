package edu.buffalo.cse562;

import net.sf.jsqlparser.schema.Column;

public interface Datum {
	
	public Object getValue();
	
	public int compare(Datum d,int index,Column[] schema);
	public Object addition(Datum d,int index);
	public Object subtraction(Datum d,int index);
	public Object multiplication(Datum d,int index);
	public Object division(Datum d,int index);

}
