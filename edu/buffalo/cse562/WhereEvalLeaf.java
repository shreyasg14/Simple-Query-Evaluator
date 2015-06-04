package edu.buffalo.cse562;

import java.sql.SQLException;
import java.util.HashMap;

import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.schema.Column;




public class WhereEvalLeaf extends Eval {
	Object[] tuple;
	HashMap<String,Integer> colIndexMap=new HashMap<String,Integer>();
	HashMap<String, String> col2Table=new HashMap<String, String>();
	
	public WhereEvalLeaf(Object[] tuple,HashMap<String,Integer> colIndexMap,HashMap<String, String> col2Table){		
		this.tuple=tuple;
		this.colIndexMap=colIndexMap;
		this.col2Table=col2Table;
	}

	@Override
	public LeafValue eval(Column arg) throws SQLException {		
		int colIndex=colIndexMap.get(arg.getColumnName());
		String tableName = col2Table.get(arg.getColumnName());
		String dataType = "";
		dataType=FromScanner.tableMeta.get(tableName+"|"+arg.getColumnName());
		LeafValue ret=null;

		if(dataType.equals("INT")){
			ret=new LongValue(Long.valueOf((String)tuple[colIndex]));
		}
		else if(dataType.equals("DECIMAL")){
			ret=new DoubleValue(Double.valueOf((String)tuple[colIndex]));
		}
		
		else if(dataType.contains("DATE")){
			StringBuilder str=new StringBuilder();
			str.append("'");
			str.append((String)tuple[colIndex]);
			str.append("'");
			ret=new DateValue(str.toString());
		}
		
		else if(dataType.contains("CHAR")){
			StringBuilder str=new StringBuilder();
			str.append("'");
			str.append((String)tuple[colIndex]);
			str.append("'");
			ret=new StringValue(str.toString());
			
		}
			
		
		
		
		
		
		
		return ret;
	
	}
}
