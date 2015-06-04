package edu.buffalo.cse562;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;

import net.sf.jsqlparser.expression.BooleanValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.FromItem;

public class SelectionOperator implements Operator{
	public static final BooleanValue FALSE = null;
	Operator input;
	Column[] schema;
	Expression condition;
	Object[] tuple;
	Datum[] tupleV2;
	Datum temp;
	List pselectList;
	static int i=0;
	HashMap<String,Integer> hashTableSchemaLength = new HashMap<String,Integer>();
	FromItem[] fi;
	Expression ex = null;
	static int count=0;
	HashMap<String, String> col2Table = new HashMap<String, String>();
	public SelectionOperator(Operator input, Column[] schema, Expression condition,List pselectList,HashMap<String,Integer> 
	hashTableSchemaLength,FromItem[] fi,HashMap<String, String> col2Table){
		this.input = input;
		this.schema = schema;
		this.condition = condition;
		this.pselectList = pselectList;
		this.hashTableSchemaLength = hashTableSchemaLength;
		this.fi = fi; 
		this.col2Table=col2Table;
		getColumnIndex4mSchema();
		}
	
	
	public Object[] readOneTuple(){
		 tuple= new Object[schema.length];
			 WhereEvalLeaf eval=null;
			 
			 
			do{
				tuple = input.readOneTuple();
				
				if(tuple == null){
					return null;
					}			
				   eval = new WhereEvalLeaf(tuple,PlainSelectEvaluator.colIndexMapforFirst,col2Table);
				   //boolean temp;
				   String s="";
					try {
						//System.out.println("type:"+eval.eval(condition).getClass());
					s= eval.eval(condition).toString();
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				  
					if(s.equals("FALSE")){
							//System.out.println("tuple is invalid");
							tuple =null;
						}
						else
						{
						//System.out.println("tuple is valid");
						return tuple;
						}
				
					
				
				
				
			}	
			while(tuple== null);
			return tuple;
	}
	
	public void reset(){
		input.reset();
	}
	
	/*public Datum[] getTuple(){
     return tuple;
	}*/
	
	public void getColumnIndex4mSchema() {
		// /Changes made Subhadeep
		int colIndex = 0;
		//////System.out.println("col name"+colName);
		

			
				/*if (colName.equals(col.toString())) {
					break;
				} else {
					colIndex++;
				}
			}
		} else {
			for (Column col : schema) {

				if (colName.equals(col.getColumnName())) {
					break;
				} else {
					colIndex++;
				}
			}
		}
		return colIndex;
	}
*/	
}


	@Override
	public Datum[] getTuple() {
		// TODO Auto-generated method stub
		return null;
	}
}


/*	Code for column elimination
 * 
 * SelectExpressionItem se = new SelectExpressionItem();
 * for(int indexSel=0;indexSel<pselectList.size();indexSel++)
{
	se = (SelectExpressionItem)pselectList.get(indexSel);
	se.getExpression().accept(eval);
	temp = eval.datum;
	tupleV2[indexSel] = temp;
	
}*/
