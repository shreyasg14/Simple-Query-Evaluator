package edu.buffalo.cse562;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import net.sf.jsqlparser.expression.AllComparisonExpression;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.InverseExpression;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.LeafValue.InvalidLeaf;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeValue;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.expression.WhenClause;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseAnd;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseOr;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseXor;
import net.sf.jsqlparser.expression.operators.arithmetic.Concat;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExistsExpression;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.Matches;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SubSelect;

public class AggregateOperator2 implements Operator {

	Column[] schema;
	HashMap<String, Object[]> aggHash;
	HashMap<String,Integer> countHash;
	Operator oper;
	PlainSelect pselect;
	String pselExp = null;
	int aggFun = 0;
	CCJSqlParser parserObject;
	Object[] oldTuple;
	Datum tempDatum;
	int colIndex = 0;
	List groupByColumn;
	int[] groupByIndexes;
	static int keySetIndex = 0;
	Set<String> keySet;
	List<String> keySetList;
	int newTupleSize = 0;
	int count;
	Object[] tempTuple;
	int[] aggIndex;
	int check=0;
	int avgCheck=0;
	Integer avgCount=new Integer(0);
	int Index =0;
	String key = "";
	boolean avgbol =false;
	boolean firstbol =false;
	boolean firstTime=true;
	Iterator<String> it;
	List pselectList;
	public AggregateOperator2(Operator oper, Column[] schema,
			PlainSelect pselect) {
		// TODO Auto-generated constructor stub

		this.schema = schema;
		this.oper = oper;
		this.pselect = pselect;
		this.newTupleSize = pselect.getSelectItems().size();
		pselectList=pselect.getSelectItems();
		aggHash = new HashMap<String, Object[]>(4);
		countHash = new HashMap<String,Integer >();
		//starcount = new HashMap<String, Integer>();
		oldTuple = new Object[schema.length];
		aggIndex = new int[newTupleSize];
		
		groupByColumn = pselect.getGroupByColumnReferences();
		if (groupByColumn != null) {
			groupByIndexes = new int[groupByColumn.size()];
			for (int groupByCount = 0; groupByCount < groupByColumn.size(); groupByCount++) {
				groupByIndexes[groupByCount] = PlainSelectEvaluator.colIndexMap.get(groupByColumn
						.get(groupByCount).toString());
			}

		}
		
		BuildHash();
		keySet = aggHash.keySet();
	    keySetList = new ArrayList<String>();
		keySetList.addAll(keySet);
		
		int count=0;
		if(avgbol){
		it=keySet.iterator();
		
		while(it.hasNext()){
		String key=it.next();
		//System.out.println("key"+key);
		tempTuple=aggHash.get(key);
		for(int i=0;i<pselectList.size();i++){
			if(pselectList.get(i).toString().contains("avg")){
				
				String avgKey=key+"|"+i;
				count=countHash.get(avgKey);
				Double avg=((Double)tempTuple[i])/(count);
				tempTuple[i]=avg;
				}
		}
		aggHash.put(key, tempTuple);
		}
		
		}
		
		
		keySet = aggHash.keySet();
	    
		it=keySet.iterator();
	}

	@Override
	public Object[] readOneTuple() {
		// TODO Auto-generated method stub
		Object[] newTuple = new Object[pselectList.size()];
		
		while(it.hasNext()){
			String str=it.next();
			newTuple=aggHash.get(str);
            return newTuple;
		}
			return null;
		

	}

	public void BuildHash() {
		StringBuilder keyBuilder = new StringBuilder();
		
		do {
			firstbol =false;
			oldTuple = oper.readOneTuple();
			count++;
			if (oldTuple == null) {
				return;
			}
            if(groupByColumn!=null){
			for (int i = 0; i < groupByIndexes.length; i++) {
				keyBuilder.append(oldTuple[groupByIndexes[i]].toString());
				key = keyBuilder.toString();
			}
            }
            else{
            	key = "1";
            }
           
			for (int pselectIndex = 0; pselectIndex < pselect.getSelectItems()
					.size(); pselectIndex++) {
				pselExp = pselectList.get(pselectIndex).toString();
				
				if (isAggregate(pselExp)) {
					aggregateMethod(SelectItemsEvaluator.aggFun[this.aggFun],
							pselectIndex, key, pselExp);
					this.aggFun++;
				} else {
					aggregateMethod(null, pselectIndex, key, pselExp);
				}

			}
			this.aggFun = 0;
			keyBuilder = new StringBuilder();
			firstbol =true;
		}   while (oldTuple != null);
	}

	public void aggregateMethod(String aggFunC, int pselectIndex, String key,
			String pselExp) {
		Double scratch = 0.0;
		if (aggFunC != null) {
			if (aggFunC.contains("sum")) {
				//System.out.println("inside sum");
				Expression exp = SelectItemsEvaluator.aggFunExpressionArray[aggFun];
				Aggregate_test aggregate_test=new Aggregate_test(schema,oldTuple);
				try {
				scratch	=aggregate_test.eval(exp).toDouble();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				catch (InvalidLeaf e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

				tempTuple = aggHash.get(key);
				if (tempTuple != null) {
					if (tempTuple[pselectIndex] != null) {
						scratch += (Double)(tempTuple[pselectIndex]);
					}
				} else {
					tempTuple = new Object[newTupleSize];
				}
				// }

				tempTuple[pselectIndex] = scratch;
				
				aggHash.put(key, tempTuple);
			}
			
			
			
			else if(aggFunC.contains("avg"))
			{
				avgbol=true;
				//System.out.println("avg");
				Expression exp = SelectItemsEvaluator.aggFunExpressionArray[aggFun];
				Aggregate_test aggregate_test=new Aggregate_test(schema,oldTuple);
				try {
						scratch	=aggregate_test.eval(exp).toDouble();
						} catch (SQLException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						} catch (InvalidLeaf e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
				}
				
				
				tempTuple = aggHash.get(key);
				//System.out.println("key is "+key);
				String avgKey=key+"|"+pselectIndex;
				avgCount = countHash.get(key+"|"+pselectIndex);
				if(tempTuple[pselectIndex]!=null){
					//System.out.println("fgfgf");
					scratch+=(Double)tempTuple[pselectIndex];
					avgCount=avgCount+1;
					
				}
				
				else{
					avgCount=1;
				}
				tempTuple[pselectIndex]=scratch;
				countHash.put(avgKey,avgCount);
				aggHash.put(key,tempTuple);
			
			
			
			
			}
			
			
			else if(aggFunC.contains("count"))
			{
			
				
				tempTuple=aggHash.get(key);
				int count=0;
				if(tempTuple[pselectIndex]==null){
					tempTuple[pselectIndex]=1;
				}
				else{
					count=(Integer)tempTuple[pselectIndex];
					count++;
					tempTuple[pselectIndex]=count;
				}
				
				aggHash.put(key, tempTuple);
				
				
				
			}
			
			
		}

		else {
		tempTuple = aggHash.get(key);
		if(tempTuple == null){
			tempTuple = new Object[newTupleSize];
			colIndex = PlainSelectEvaluator.colIndexMap.get(pselExp);
			tempTuple[pselectIndex] = oldTuple[colIndex];
			aggHash.put(key, tempTuple);
		}
		else{
            	colIndex = PlainSelectEvaluator.colIndexMap.get(pselExp);
				tempTuple[pselectIndex] = oldTuple[colIndex];
				aggHash.put(key, tempTuple);
			}

		}

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

	boolean isAggregate(String str) {
		if (str.contains("sum") || str.contains("avg")
				|| str.contains("count")) {
			return true;
		}
		return false;
	}

	
	public int getColumnIndex4mSchema(String colName) {
		// /Changes made Subhadeep
		int colIndex = 0;
		if (colName.contains(".")) {

			for (Column col : schema) {
				if (colName.equals(col.toString())) {
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

}
