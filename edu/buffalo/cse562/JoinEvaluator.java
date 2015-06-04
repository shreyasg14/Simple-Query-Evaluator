package edu.buffalo.cse562;

import java.sql.SQLException;

import net.sf.jsqlparser.expression.AllComparisonExpression;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.InverseExpression;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.LeafValue;
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
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SubSelect;

public class JoinEvaluator extends Eval {

	Column[] schema;
	Object[] tuple;
	
	public JoinEvaluator(Column[] schema, Object[] tuple) {
		// TODO Auto-generated constructor stub
		this.schema=schema;
		this.tuple=tuple;
		
	}

	@Override
	public LeafValue eval(Column arg) throws SQLException {
		int colIndex=getColumnIndex4mSchema(arg.getColumnName().toString());
		String tableName = PlainSelectEvaluator.col2Table.get(arg.getColumnName());
		
		String dataType = "";
		dataType=FromScanner.tableMeta.get(tableName+"|"+arg.getColumnName());
		//System.out.println("Column:"+arg.getColumnName());
		LeafValue ret=null;

		if(dataType.contains("int")||dataType.contains("INT")){
			//System.out.println("inside int");
			ret=new LongValue(Long.parseLong(tuple[colIndex].toString()));
		}
		else if(dataType.contains("decimal")||dataType.contains("DECIMAL")){
			ret=new DoubleValue(Double.parseDouble(tuple[colIndex].toString()));
		}
		
		else if(dataType.contains("date")||dataType.contains("DATE")){
			StringBuilder str=new StringBuilder();
			str.append("'");
			str.append(tuple[colIndex].toString());
			str.append("'");
			ret=new DateValue(str.toString());
		}
		
		else if(dataType.contains("char(")||dataType.contains("CHAR(")||
				dataType.contains("char")||dataType.contains("CHAR")||
				dataType.contains("varchar(")||dataType.contains("VARCHAR(")||
				dataType.contains("VARCHAR")||dataType.contains("VARCHAR")){
			StringBuilder str=new StringBuilder();
			str.append("'");
			str.append(tuple[colIndex].toString());
			str.append("'");
			ret=new StringValue(str.toString());
			
		}
			
		
		
		
		
		
		
		return ret;
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
