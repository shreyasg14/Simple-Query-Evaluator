package edu.buffalo.cse562;

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
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SubSelect;

public class SelectItemsEvaluator implements ExpressionVisitor {

	public int aggFlag;
	List<Expression> aggFunExpression;
	static Expression[] aggFunExpressionArray = new Expression[10];
	public static int aggFunCount = 0;
	public static int aggCheck=0;
	Column[] schema;
	Operator oper;
	public Column[] aggColumn;
	public static String[] aggFun;
	//public Column[] aggColumn;
	public Object[] datums;
	public Datum datum=new DatumDirectory();
	PlainSelect pselect;
	String aliasName;
	int aliasIndex;
	int index;
	Object[] tuple;
	public SelectItemsEvaluator(Column[] schema,Operator oper,PlainSelect pselect,String aliasName,Object[] tuple, int aliasIndex){
		this.pselect=pselect;
		this.schema=schema;
		this.oper=oper;
		this.tuple=tuple;
		this.aliasName = aliasName;
		this.aliasIndex = aliasIndex;
		if(aggFunCount==0){
		aggFun=new String[pselect.getSelectItems().size()];}
		aggColumn=new Column[pselect.getSelectItems().size()];
	}
/*	public SelectItemsEvaluator(Column[] schema,Operator oper,PlainSelect pselect){
		this.pselect=pselect;
		this.schema=schema;
		this.oper=oper;
		
	}*/
	
	public void visit(NullValue arg0) {
		// TODO Auto-generated method stub
		
	}

	public void visit(Function value) {
		//////System.out.println("inside function");
		aggFlag=1;
		aggCheck=1;
		aggFun[aggFunCount++]=value.getName();
		ExpressionList s=value.getParameters();
		if(s==null){	
		}		
		if(s!=null){
		aggFunExpression=s.getExpressions();
		////////System.out.println("Function to be added:"+aggFun[aggFunCount-1]);
		aggFunExpressionArray[aggFunCount-1] = aggFunExpression.get(0);
		}
		else if(s==null && value.isAllColumns()){
			aggColumn[0]=schema[0];
		}
		aggFlag=0;
		//aggFunExpression.get(0).accept(this);
		
	}

		
		// TODO Auto-generated method stub
	

	public void visit(InverseExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	public void visit(JdbcParameter arg0) {
		// TODO Auto-generated method stub
		
	}

	public void visit(DoubleValue value) {
		// TODO Auto-generated method stub
		datum = new DatumDirectory(value);
	}

	public void visit(LongValue value) {
		// TODO Auto-generated method stub
		datum = new DatumDirectory(value);
	}

	public void visit(DateValue value) {
		// TODO Auto-generated method stub
		datum = new DatumDirectory(value);
	}

	public void visit(TimeValue arg0) {
		// TODO Auto-generated method stub
		
	}

	public void visit(TimestampValue arg0) {
		// TODO Auto-generated method stub
		
	}

	public void visit(Parenthesis arg) {
		//////System.out.println("paranthesis"+arg.toString());
		
		arg.getExpression().accept(this);
		
	}

	public void visit(StringValue value) {
		// TODO Auto-generated method stub
		datum = new DatumDirectory(value);
	}

	public void visit(Addition exp) {
		// TODO Auto-generated method stub
		Datum leftValue, rightValue;
		exp.getLeftExpression().accept(this);
		leftValue = datum;
		exp.getRightExpression().accept(this);
		rightValue = datum;
		datum = new DatumDirectory(leftValue.addition(rightValue,index));
		
	}

	public void visit(Division arg0) {
		// TODO Auto-generated method stub
		
	}

	public void visit(Multiplication arg0) {
		// TODO Auto-generated method stub
		
	}

	public void visit(Subtraction exp) {
		// TODO Auto-generated method stub
		//////System.out.println("subtraction"+exp.toString());
		Datum leftValue, rightValue;
		exp.getLeftExpression().accept(this);
		leftValue = datum;
		//////System.out.println("left:"+leftValue.getValue().toString());
		exp.getRightExpression().accept(this);
		rightValue = datum;
		//////System.out.println("right:"+rightValue.getValue().toString());
		datum = new DatumDirectory(leftValue.subtraction(rightValue,index));
		
		
	}

	public void visit(AndExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	public void visit(OrExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	public void visit(Between arg0) {
		// TODO Auto-generated method stub
		
	}

	public void visit(EqualsTo arg0) {
		// TODO Auto-generated method stub
		
	}

	public void visit(GreaterThan arg0) {
		// TODO Auto-generated method stub
		
	}

	public void visit(GreaterThanEquals arg0) {
		// TODO Auto-generated method stub
		
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

	public void visit(MinorThan arg0) {
		// TODO Auto-generated method stub
		
	}

	public void visit(MinorThanEquals arg0) {
		// TODO Auto-generated method stub
		
	}

	public void visit(NotEqualsTo arg0) {
		// TODO Auto-generated method stub
		
	}

	public void visit(Column value) {
		if(aliasName!=null){
			value = schema[aliasIndex];
		}
		
		if(aggFlag==1){
			
			aggColumn[aggFunCount-1]=value;
		
			
				}
				
				else{
					////////System.out.println("column");
					index=getColumnIndex4mSchema(value.toString());
					////////////System.out.println("inside col else");
					////////System.out.println("Column:"+value.toString());
					int flag1=0;
					if(value.getTable().getName()==null){
					 	
					}
				//	datums = oper.readOneTuple();
					for(Column c:schema){
						////////////System.out.println("schema:"+c.getColumnName());
						if(c.getColumnName().equals(value.getColumnName())){
							////////////System.out.println("FLAG::"+flag1);
							break;
						}			
						flag1++;
					}
				//	while(datums!=null)
					
					////////////System.out.println("datums::"+datums[flag1].getValue().toString());
					if(tuple!=null)
					datum=new DatumDirectory(tuple[flag1]);
					//datums = oper.readOneTuple();
				
					//oper.reset();
					////////////System.out.println("datums::"+datums[flag1].getValue().toString());
				}		
					
				}

		// TODO Auto-generated method stub
		
	

	public void visit(SubSelect arg0) {
		// TODO Auto-generated method stub
		
	}

	public void visit(CaseExpression arg) {
		// TODO Auto-generated method stub
		//System.out.println("inside case"+arg.toString());
        List whenClauses=arg.getWhenClauses();
        Expression expElse=(Expression)arg.getElseExpression();
        for(int i=0;i<whenClauses.size();i++){
        Expression exp=(Expression)whenClauses.get(i);
        exp.accept(this);
        }
        
		//arg.accept(this);
		
	}

	public void visit(WhenClause arg) {
		// TODO Auto-generated method stub
		Expression exp=arg.getThenExpression();
		//System.out.println("whenexpression"+arg.getWhenExpression().toString());
		//System.out.println("inside when"+arg.toString());
		
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
	public int getColumnIndex4mSchema(String colName) {
		// /Changes made Subhadeep
		int colIndex = 0;
		////////System.out.println("col name"+colName);
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
