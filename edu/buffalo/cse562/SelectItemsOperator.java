package edu.buffalo.cse562;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;

public class SelectItemsOperator implements Operator {

	Operator oper;
	Column[]schema;
	PlainSelect pselect;
	String alias;
	Object[] tuple;
	int aliasIndex;
	Expression expression;
	Object[] tuple1;
	 public SelectItemsOperator(Operator oper,Column[] schema,PlainSelect pselect,SelectExpressionItem se,int aliasIndex) {
		 this.oper=oper;
		 this.schema=schema;
		 //this.alias=alias;
		 this.aliasIndex=aliasIndex;
		 this.pselect=pselect;
		 this.expression=se.getExpression();
		}
	
	@Override
	public Object[] readOneTuple() {
		System.out.println("inside selection operator");
		tuple1= new Object[schema.length];
		
		tuple=new Object[schema.length];
		//System.out.println("schema.length"+tuple.length);
		SelectItemsEvaluator se1=null;
		do{
			//System.out.println("inside exp eval");
			tuple=oper.readOneTuple();
			if(tuple==null)
				return null;
			
			
			////System.out.println("inside "+expression.toString());
			se1=new SelectItemsEvaluator(schema, oper, pselect, alias, tuple,aliasIndex);
			expression.accept(se1);
			for(int i=0;i<tuple.length;i++){
				tuple1[i]=tuple[i];
			}
			tuple1[schema.length-1]=se1.datum;
			//System.out.println("final value:"+se1.datum.getValue().toString());
			return tuple1;
			//return tuple;
			
	}	
		while(tuple==null);
		
	
			
	}
	@Override
	public void reset() {
		// TODO Auto-generated method stub

	}

	@Override
	public Datum[] getTuple() {
		// TODO Auto-generated method stub
		return null;
	}

}
