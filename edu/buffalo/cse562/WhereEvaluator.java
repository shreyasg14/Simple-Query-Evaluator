package edu.buffalo.cse562;

import java.util.HashMap;
import java.util.List;

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
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.Matches;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItemVisitor;
import net.sf.jsqlparser.statement.select.SubSelect;

public class WhereEvaluator implements ExpressionVisitor, SelectItemVisitor {

	public int flag = 0;
	public Datum datum = new DatumDirectory();
	//public Datum[] datums;
	public Object[] datums;
	public Column[] schema;
	HashMap<String, String> map = new HashMap<String, String>();
	private String[] alias;
	static int check = 0;
	HashMap<String, Integer> hashTableSchemaLength = new HashMap<String, Integer>();
	FromItem[] fi;
	public int aggFlag;
	public int aggCheck;
	Column aggColumn;
	public String aggFun;
	static int count=0;
	Operator oper = null;
	int colIndex = 0; 
	HashMap<String, Integer> colIndexMap = new HashMap<String,Integer>();
	//Changes made by Subhadeep
	public int index = 0;
	
	public WhereEvaluator(Column[] schema, Object[] tuple,
			HashMap<String, Integer> hashTableSchemaLength, FromItem[] fi,
			Operator oper,HashMap<String, Integer> colIndexMap ) {
		this.hashTableSchemaLength = hashTableSchemaLength;
		this.schema = schema;
		datums = tuple;
		this.colIndexMap = colIndexMap;
		/*for(Datum d: datums){
			////System.out.println(d.getValue().toString());
		}*/
		this.oper = oper;
		this.fi = fi;
		/*for (int i = 0; i < schema.length; i++) {

			map.put(schema[i].getTable().getAlias(), schema[i].getTable()
					.getName());
		}
*///getColumnIndex4mSchema();
//colIndexMap.put("shipdate", 10);
		
	}

	public WhereEvaluator(Column[] schema, Operator oper) {
		this.schema = schema;
		this.oper = oper;

	}

	public void visit(GreaterThan exp) {
		// (Column)exp;

		// TODO Auto-generated method stub
		Datum temp;
		Datum temp2;
		flag = 0;
		check++;
		/*
		 * if(check%2==0) alias[1]=exp.
		 */
		exp.getLeftExpression().accept(this);
		// setAlias(exp.getLeftExpression());
		temp = datum;
		////////System.out.println("temp:"+temp.getValue().toString());
		
		// //////System.out.println("4====="+exp.getRightExpression().toString());
		exp.getRightExpression().accept(this);
		setAlias(exp.getRightExpression());
		temp2 = datum;
		////////System.out.println("temp:"+temp.getValue().toString());
		// //////System.out.println("greater than");
		if (temp.compare(temp2,colIndex,schema) > 0) {
			// //////System.out.println("setting flag!!!!");
			flag = 1;
		}
		////////System.out.println("temp2=="+temp2.getValue().toString()+"temp1==="+temp.getValue().toString()+"  Flag"+flag);
		
	}

	public void visit(DoubleValue value) {
		// TODO Auto-generated method stub
		datum = new DatumDirectory(value);
	}

	public void visit(LongValue value) {
		// TODO Auto-generated method stub
		datum = new DatumDirectory(value);
	}

	public void visit(StringValue value) {
		// TODO Auto-generated method stub
		//////System.out.println("string"+value.toString());
		datum = new DatumDirectory(value);
	}

	public void visit(NullValue arg0) {

	}

	public void visit(Function value) {
		ExpressionList list= value.getParameters();
		List<Expression> l=list.getExpressions();
		//System.out.println("l.get(0)"+l.get(0).toString());
		l.get(0).accept(this);
		
	}

	public void visit(InverseExpression arg0) {
		// TODO Auto-generated method stub

	}

	public void visit(JdbcParameter arg0) {
		// TODO Auto-generated method stub

	}

	public void visit(DateValue date) {
		// TODO Auto-generated method stub
		datum=new DatumDirectory(date);
		
		
	}

	public void visit(TimeValue arg0) {
		// TODO Auto-generated method stub

	}

	public void visit(TimestampValue arg0) {
		// TODO Auto-generated method stub

	}

	public void visit(Parenthesis arg) {
		////////System.out.println("Paranthesis!!!");
		// TODO Auto-generated method stub
		//arg.accept(this);
		arg.getExpression().accept(this);

	}

	public void visit(Addition exp) {
		////////System.out.println("Inside addition");
		// TODO Auto-generated method stub
		Datum leftValue, rightValue;
		exp.getLeftExpression().accept(this);
		leftValue = datum;
		exp.getRightExpression().accept(this);
		rightValue = datum;
		datum = new DatumDirectory(leftValue.addition(rightValue,index));
		////////System.out.println("After addition");
		// //////System.out.println("MOFOS");
		// String dataType = "int";
		//
		// switch(dataType){
		// case "int":
		// default:break;
		// }
		//
		// int b = Integer.parseInt(leftValue.getValue().toString()) +
		// Integer.parseInt(rightValue.getValue().toString());
		// datum = new DatumDirectory(b);
	}

	public void visit(Division exp) {
		// TODO Auto-generated method stub
		Datum leftValue, rightValue;
		exp.getLeftExpression().accept(this);
		leftValue = datum;
		exp.getRightExpression().accept(this);
		rightValue = datum;
		datum = new DatumDirectory(leftValue.division(rightValue,index));
		/*
		 * int b = Integer.parseInt(leftValue.getValue().toString()) /
		 * Integer.parseInt(rightValue.getValue().toString()); datum = new
		 * DatumDirectory(b);
		 */
	}

	public void visit(Multiplication exp) {
		// TODO Auto-generated method stub
		Datum leftValue, rightValue;
		exp.getLeftExpression().accept(this);
		leftValue = datum;
		exp.getRightExpression().accept(this);
		rightValue = datum;
		datum = new DatumDirectory(leftValue.multiplication(rightValue,index));

		/*
		 * int b = Integer.parseInt(leftValue.getValue().toString()) *
		 * Integer.parseInt(rightValue.getValue().toString()); datum = new
		 * DatumDirectory(b);
		 */
	}

	public void visit(Subtraction exp) {
		// TODO Auto-generated method stub
		Datum leftValue, rightValue;
		exp.getLeftExpression().accept(this);
		leftValue = datum;
		exp.getRightExpression().accept(this);
		rightValue = datum;
		datum = new DatumDirectory(leftValue.subtraction(rightValue,index));

		/*
		 * int b = Integer.parseInt(leftValue.getValue().toString()) -
		 * Integer.parseInt(rightValue.getValue().toString()); datum = new
		 * DatumDirectory(b);
		 */}

	public void visit(AndExpression exp) {

		int flagTemp = 0;
		exp.getLeftExpression().accept(this);
		flagTemp = flag;
		exp.getRightExpression().accept(this);
		if (flag == 1 && flagTemp == 1) {
			flag = 1;
		} else {
			flag = 0;
		}

	}

	public void visit(OrExpression exp) {
		int flagTemp = 0;
		exp.getLeftExpression().accept(this);
		flagTemp = flag;
		exp.getRightExpression().accept(this);
		
		if (flag == 1 || flagTemp == 1) {
			flag = 1;
		} else {
			flag = 0;
		}

	}

	public void visit(Between arg0) {
		// TODO Auto-generated method stub

	}

	public void visit(EqualsTo exp) {
		// TODO Auto-generated method stub

		Datum temp;
		Datum temp2;
		flag = 0;
		// //////System.out.println("Baby YEs Baby");
		exp.getLeftExpression().accept(this);
		temp = datum;
		exp.getRightExpression().accept(this);
		temp2 = datum;
		//////System.out.println("index"+index);
		// //////System.out.println("value"+ datum.compare(temp));
			if (temp.compare(temp2,index,schema) == 0) {
				/*////System.out.println("value: " + temp.getValue().toString() +
						 " temp: " + temp2.getValue().toString());
			*/			
						
			flag = 1;
			//////System.out.println("matched");
		}
	}

	public void visit(GreaterThanEquals exp) {
		// TODO Auto-generated method stub
		// TODO Auto-generated method stub
		Datum temp;
		Datum temp2;
		flag = 0;
		// //////System.out.println("Baby YEs Baby");
		exp.getLeftExpression().accept(this);
		temp = datum;
		exp.getRightExpression().accept(this);
		temp2 = datum;
		// //////System.out.println("value"+ datum.compare(temp));
		////System.out.println("value: " + temp.getValue().toString() +
		// " temp: " + temp2.getValue().toString());
		if (temp.compare(temp2,index,schema) >= 0) {
			////System.out.println("flag==1");
			flag = 1;
		}

	}

	public void visit(InExpression arg0) {
		// TODO Auto-generated method stub

	}

	public void visit(IsNullExpression arg0) {
		// TODO Auto-generated method stub

	}

	public void visit(LikeExpression arg0) {
		// TODO Auto-generated method stub

	}

	public void visit(MinorThan exp) {
		// TODO Auto-generated method stub
		// TODO Auto-generated method stub
		Datum temp;
		Datum temp2;
		flag = 0;
		
	count++;	
		exp.getLeftExpression().accept(this);
		temp = datum;
		exp.getRightExpression().accept(this);
		temp2 = datum;
		// //////System.out.println("value"+ datum.compare(temp));
		// //////System.out.println("value: " + temp2.getValue().toString() +
		// " temp: " + temp.getValue().toString());
		if (temp.compare(temp2,index,schema) < 0) {
			flag = 1;
		}

	}

	public void visit(MinorThanEquals exp) {
		// TODO Auto-generated method stub
		Datum temp;
		Datum temp2;
		flag = 0;
		// //////System.out.println("Baby YEs Baby");
		exp.getLeftExpression().accept(this);
		temp = datum;
		exp.getRightExpression().accept(this);
		temp2 = datum;
		//System.out.println("value"+ datum.compare(temp));
		//System.out.println("value: " + temp2.getValue().toString() +
		// " temp: " + temp.getValue().toString());
		//////System.out.println("index"+index);
		if (temp.compare(temp2,index,schema) <= 0) {
			//System.out.println("flag");
			flag = 1;
		}
	}

	public void visit(NotEqualsTo exp) {
		// TODO Auto-generated method stub
		////System.out.println("not equals to");
		Datum temp;
		Datum temp2;
		flag = 0;
		////System.out.println("exp:"+exp.toString());
		// //////System.out.println("Baby YEs Baby");
		exp.getLeftExpression().accept(this);
		temp = datum;
		exp.getRightExpression().accept(this);
		temp2 = datum;
		// //////System.out.println("value"+ datum.compare(temp));
		////System.out.println("value: " + temp2.getValue().toString() + " temp: " + temp.getValue().toString());
		////System.out.println("index"+index);
		//if(count==0){
			
		//}
		if (temp.compare(temp2,index,schema) != 0) {
			flag = 1;
		}

	}

	public void visit(Column value) {
		// //////System.out.println("datums:::"+datums[0].getValue().toString());
       //////System.out.println("Column:"+ value.toString());
		//System.out.println("col value:"+value.toString());
		colIndex = colIndexMap.get(value.toString());
		
		index=colIndex;
		//////System.out.println("colIndex"+colIndex);
		////////System.out.println("Column Index WHere evealu:"+colIndex + value.toString());
		datum =new DatumDirectory(datums[colIndex]);
		////////System.out.println("Column Index Value:"+colIndex);
/*		if (value.toString().contains(".")) {
	
			 // //////System.out.println("datums::"+datums[flag1].getValue().toString());
		}*/
	}

	public void visit(SubSelect arg0) {
		// TODO Auto-generated method stub

	}

	public void visit(CaseExpression arg0) {
		// TODO Auto-generated method stub
		//System.out.println("inside case");

	}

	public void visit(WhenClause arg0) {
		// TODO Auto-generated method stub
		//System.out.println("inside when");

	}

	public void visit(ExistsExpression arg0) {
		// TODO Auto-generated method stub

	}

	public void visit(AllComparisonExpression arg0) {
		// TODO Auto-generated method stub

	}

	public void visit(AnyComparisonExpression arg0) {
		// TODO Auto-generated method stub

	}

	public void visit(Concat arg0) {
		// TODO Auto-generated method stub

	}

	public void visit(Matches arg0) {
		// TODO Auto-generated method stub

	}

	public void visit(BitwiseAnd arg0) {
		// TODO Auto-generated method stub

	}

	public void visit(BitwiseOr arg0) {
		// TODO Auto-generated method stub

	}

	public void visit(BitwiseXor arg0) {
		// TODO Auto-generated method stub

	}

	public void visit(AllColumns arg0) {
		// TODO Auto-generated method stub

	}

	public void visit(AllTableColumns arg0) {
		// TODO Auto-generated method stub

	}

	public void visit(SelectExpressionItem arg0) {
		// TODO Auto-generated method stub
	//	//////System.out.println("Hi MC Chutitya mein yahan houn");
	}

	public void setAlias(Expression exp) {

	}

	public String[] getAlias() {
		return alias;
	}
	
}
