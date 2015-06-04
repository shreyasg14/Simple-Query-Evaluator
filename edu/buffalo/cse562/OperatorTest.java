package edu.buffalo.cse562;

import java.io.File;

import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.schema.Column;

public class OperatorTest{
	
	public static void dump(Operator input){
		//System.out.println("inside dump");
		//System.out.println("input::"+(input instanceof ScanOperator));
		Object[] row = input.readOneTuple();
		long countTuple = PlainSelectEvaluator.rowCount;
		long count=0;
		while(row!=null){
			count++;
			if(countTuple>0){
			if(count>countTuple)
			break;
			}
			System.out.print(row[0].toString());
			for(int i=1;i<row.length;i++){
				System.out.print("|"+row[i].toString());
				
			}
			
		   System.out.println("");
			
			row = input.readOneTuple();
		}
		//System.out.println("Number of Tuples:"+count);
		//System.gc();
	}
}
