package edu.buffalo.cse562;

import java.sql.SQLException;

import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.PlainSelect;

public class Aggregate_test extends Eval {

     Object[] tuple;	
     Column[] schema;
	 public Aggregate_test(Column[] schema,
	  	Object[] tuple) {
	    this.schema = schema;
	    this.tuple=tuple;
		
	}
	
	@Override
	public LeafValue eval(Column arg) throws SQLException {
		int colIndex=PlainSelectEvaluator.colIndexMap.get(arg.toString());
		String dataType=FromScanner.tableMeta.get(schema[colIndex].getTable().getName()+"|"+schema[colIndex].getColumnName());
		LeafValue ret=null;
		
		if(dataType.contains("DECIMAL")||dataType.contains("INT")){
		ret=new DoubleValue(Double.valueOf((String)(tuple[colIndex])));
		}
		
		else{
			StringBuilder str=new StringBuilder();
			str.append("'");
			str.append((String)tuple[colIndex]);
			str.append("'");
			ret=new StringValue(str.toString());
		}
		
		
		return ret;
		}
	
	
	

}
