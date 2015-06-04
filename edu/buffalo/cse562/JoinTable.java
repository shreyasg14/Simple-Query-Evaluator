package edu.buffalo.cse562;

import java.io.StringReader;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.Spliterator;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;

public class JoinTable implements Operator {
	
	Column[] leftSchema;
	Operator leftOper;
	FromItem[] fromItem;
	HashMap<String,List<Object[]>> initialMap=new HashMap<String,List<Object[]>>();
	HashMap<String, Integer> colIndexMap = new HashMap<String,Integer>();
	FromScanner fs;
	String clause=null;
	int flag=0;
	String key;
	public static Column[] joinedSchema;
	int index=0;
	HashMap<String,List<Object[]>> finalMap=new HashMap<String, List<Object[]>>();
	HashMap<String,List<Object[]>> tempMap=new HashMap<String, List<Object[]>>();
	HashMap<String, String> col2Table=new HashMap<String, String>();
	List<Object[]> list=new ArrayList<Object[]>();
	Integer[] nextIndexArr;
	Column[] tempSchema;
	String result="FALSE";
	private boolean check=false;
	int joinCheck=0;
	private boolean wrongJoinCheck=false;
	private boolean singleKey;
	public JoinTable(Operator oper,Column[] schema,FromItem[] fromItem,FromScanner fs){
		leftOper=oper;
		leftSchema=schema;
		tempSchema=schema;
		this.fromItem=fromItem;
		this.fs=fs;
		buildInitialHash();
		oper.reset();
		buildFinalHash();
		
		
		
		
	}

	private void buildFinalHash() {
		for(int i=1;i<fromItem.length;i++){
			int[] arr=new int[2];
			////System.out.println("from table "+fromItem[i].toString()+ " i "+i);
			fromItem[i].accept(fs);
			Operator rightOper=fs.source;
			rightOper.reset();
			Column[] rightSchema=FromScanner.schema;
			Expression ex=buildQuery(fromItem[i].toString());
			if(ex!=null){
				buildHashMap(rightSchema);
				//Set<String> keyset=colIndexMap.keySet();
					
			}
			int index=0;
			String colName=null;
			String str=PlainSelectEvaluator.joinMap.get(fromItem[i-1].toString()+"|"+fromItem[i].toString());
			if(str!=null)
			colName=str.split("=")[1].split("\\.")[1];
			if(colName==null){
				colName=PlainSelectEvaluator.joinMap.get(fromItem[0].toString()+"|"+fromItem[i].toString()).split("=")[1].split("\\.")[1].trim();
			    //System.out.println("colName"+colName);
			}
			
			if(i==joinCheck){
				////System.out.println("comeon man");
				int sIndex=0;
				for(int j=i;j>=0;j--){
					String key=PlainSelectEvaluator.joinMap.get(fromItem[j].toString()+"|"+fromItem[i].toString());
					////System.out.println("key"+key);
					if(key!=null){
						key=key.split("=")[1].trim().split("\\.")[1];
						////System.out.println("key not null");
						int aIndex=0;
						for(Column c:rightSchema){
							////System.out.println("column name"+c.getColumnName());
							  if(c.getColumnName().equals(key)){
							     break;	  
							  }
							aIndex++;
							}
						////System.out.println("lIndex"+aIndex);
						arr[sIndex++]=aIndex;
						
					}
					
				}
				
			}
			for(Column c:rightSchema){
				if(c.getColumnName().equals(colName.trim())){
					break;
					
				}
				index++;
			}
				joinedSchema=concat(tempSchema,rightSchema);
			tempSchema=joinedSchema;
			int nextIndex=8888;
			if(i<fromItem.length-1){
			nextIndexArr=getNextIndex(i+1,joinedSchema,rightSchema);
			
			if(nextIndexArr[1]==null){
				singleKey=true;
				nextIndex=nextIndexArr[0].intValue();
				//nextIndex=nextIndexArr[0].intValue();
			}
			
			else{
				////System.out.println("false");
				////System.out.println("dsj"+nextIndexArr[0]+nextIndexArr[1]);
				joinCheck=i+1;
				singleKey=false;
				////System.out.println("join check"+joinCheck);
				////System.out.println("keeee"+nextIndexArr[0]+nextIndexArr[1]+  i);
				//nextIndex=nextIndexArr[0]+"|"+nextIndexArr[1];
			}
			}
			Object[] tuple=rightOper.readOneTuple();
			result="TRUE";
			
			while(tuple!=null){
            		   
			if(ex!=null){
				 WhereEvalLeaf eval=new WhereEvalLeaf(tuple, colIndexMap,col2Table);
			   try {
				result=eval.eval(ex).toString();
			  } catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
				
		    }
		    
			if(result=="TRUE"){
			////System.out.println("index for next table is"+nextIndex);
			List<Object[]> leftTuple=null;
		
			if(initialMap!=null){
				leftTuple=initialMap.get(tuple[index].toString());
				////System.out.println("left index"+);
			
			
			}
			
			
			else if(check==true){
			////System.out.println("reading temp map"+ i);
				if(joinCheck!=i)
				leftTuple=tempMap.get(tuple[index].toString());
				else{
					////System.out.println("for i:"+i);
					////System.out.println("sd"+nextIndexArr[2].toString()+ "   "+nextIndexArr[3].toString());
					leftTuple=tempMap.get(tuple[arr[0]].toString()+"|"+tuple[arr[1]].toString());
				}
			}
			
			else{
				////System.out.println("reading from final map"+i);
				if(joinCheck!=i)
					leftTuple=finalMap.get(tuple[index].toString());
					else{
						////System.out.println("for i:"+i);
						////System.out.println("sd"+nextIndexArr[2].toString()+ "   "+nextIndexArr[3].toString());
						////System.out.println("key now"+tuple[arr[0]].toString()+"|"+tuple[arr[1]].toString()+ "  i"+i);
						leftTuple=finalMap.get(tuple[arr[0]].toString()+"|"+tuple[arr[1]].toString());
					}
			}
			
			if(leftTuple!=null){ 
				////System.out.println("matched");
				
				for(int k=0;k<leftTuple.size();k++){
				Object[] tempTuple=concat(leftTuple.get(k),tuple);
				////System.out.println("tupleIndex"+tuple[index].toString());
				////System.out.println("left "+tempTuple[0].toString());
				
				String key=null;
				if(i==fromItem.length-1){
					////System.out.println("last index");
					list.add(tempTuple);
				}
				
				else{
				
				if(check==false){
				////System.out.println("inside temp" +i);
				//////System.out.println("wrong"+wrongJoinCheck);
				List<Object[]> tempList=null;
				if(!singleKey){
				key=tempTuple[nextIndexArr[0]].toString()+"|"+tuple[nextIndexArr[1]].toString();
				////System.out.println("key"+key + "i");
				tempList=tempMap.get(key);
				}
				else
				tempList=tempMap.get(tempTuple[nextIndex].toString());
				
				////System.out.println("index "+nextIndex+i);
				
				if(tempList==null){
					tempList=new ArrayList<Object[]>();
					tempList.add(tempTuple);
					if(singleKey)
					tempMap.put(tempTuple[nextIndex].toString(), tempList);	
					else
						tempMap.put(tempTuple[nextIndexArr[0]].toString()+"|"+tempTuple[nextIndexArr[1]].toString(), tempList);
					
					}
				
				else{
				   tempList.add(tempTuple);
				   if(singleKey){
					    tempMap.put(tempTuple[nextIndex].toString(), tempList);	
					}
				   else{
					   tempMap.put(tempTuple[nextIndexArr[0]].toString()+"|"+tempTuple[nextIndexArr[1]].toString(), tempList);
										
				   }
				   }
				}
				
				else{
				List<Object[]> tempList=null;
				if(!singleKey){
					
				key=tempTuple[nextIndexArr[0]].toString()+"|"+tempTuple[nextIndexArr[1]].toString();
				////System.out.println("key"+key + "i");
				tempList=finalMap.get(key);
				}
				else
				tempList=finalMap.get(tempTuple[nextIndex].toString());
				
				if(tempList==null){
					tempList=new ArrayList<Object[]>();
					tempList.add(tempTuple);
					if(singleKey)
					finalMap.put(tempTuple[nextIndex].toString(), tempList);	
					else
						finalMap.put(tempTuple[nextIndexArr[0]].toString()+"|"+tempTuple[nextIndexArr[1]].toString(), tempList);
					
					}
				
				else{
				   tempList.add(tempTuple);
				   if(singleKey){
					   ////System.out.println("key"+leftTuple.get(k)[nextIndex].toString());
						
					   finalMap.put(tempTuple[nextIndex].toString(), tempList);	
					}
				   else{
					   finalMap.put(tempTuple[nextIndexArr[0]].toString()+"|"+leftTuple.get(k)[nextIndexArr[1]].toString(), tempList);
										
				   }
				   }
				}
				
				
				
			}
			}
			}
			
			}
			tuple=rightOper.readOneTuple();
			}
			
			if(check==false){
				////System.out.println("final map reset "+  i);
				check=true;
				finalMap=null;
				finalMap=new HashMap<String, List<Object[]>>();
			}
			
			else{
				tempMap=null;
				////System.out.println("temp map reset "+i);
				check=false;
				tempMap=new HashMap<String, List<Object[]>>();
			
			}
			
			initialMap=null;
		}
		
	}
		
	

	private Integer[] getNextIndex(int i,Column[] joinedSchema,Column[] rightSchema) {
		wrongJoinCheck=false;
		Integer newIndex[]=new Integer[4];
		int localIndex=0,aIndex=0;
		String next=PlainSelectEvaluator.joinMap.get(fromItem[i-1].toString()+"|"+fromItem[i].toString());
		////System.out.println("next"+next);
		if(i<fromItem.length-1){
			String value=next.split("=")[0].split("\\.")[1].trim();
			////System.out.println("value"+value);
			for(Column c:joinedSchema){
				  if(c.getColumnName().equals(value)){
				     break;	  
				  }
				localIndex++;
				}
			newIndex[0]=localIndex;
			
		}
		
		else{
			////System.out.println("inside else");
			int lIndex=0;
			int sIndex=2;
			for(int j=i;j>=0;j--){
				
				
				localIndex=0;
				aIndex=0;
				String str=PlainSelectEvaluator.joinMap.get(fromItem[j].toString()+"|"+fromItem[i].toString());
				////System.out.println("str....."+str);
				if(str!=null){
					String value=str.split("=")[0].split("\\.")[1].trim();
					String value1=str.split("=")[1].split("\\.")[1].trim();
					////System.out.println("value"+value);
					for(Column c:joinedSchema){
						////System.out.println("column name"+c.getColumnName());
						  if(c.getColumnName().equals(value)){
						     break;	  
						  }
						localIndex++;
						}
					////System.out.println("lIndex"+localIndex);
					newIndex[lIndex++]=localIndex;
					
					
					
					
					
					
				}
				
				
			}
			
			
		}
		
		return newIndex;
	}

	private Expression getWhereExpression(String query) {
		CCJSqlParser tempParser = new CCJSqlParser(new StringReader(query));
		Statement stmt = null;
		try {
			stmt = tempParser.Statement();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    if(stmt instanceof Select){
	    	SelectBody select = ((Select)stmt).getSelectBody();
	    	if(select instanceof PlainSelect){
				PlainSelect pselect = (PlainSelect)select;
				return pselect.getWhere();
	    	}	
	    	}
	    
	    
	return null;	
	}

	private void buildInitialHash() {
		String first=fromItem[0].toString();
		String second=fromItem[1].toString();
		String condition=PlainSelectEvaluator.joinMap.get(first+"|"+second);
		String indexTerm=null;
		if(condition!=null){
			indexTerm=condition.split("=")[0].split("\\.")[1];
		}
		
		
		int index=0;
		for(int i=0;i<leftSchema.length;i++){
			if(indexTerm.trim().equals(leftSchema[i].getColumnName().trim())){			
				index=i;
				break;
			}
			
		}
		
		//System.out.println("index"+index);
		////////System.out.println("index is"+index);	
		leftOper.reset();
		Object[] tuple=leftOper.readOneTuple();
		while(tuple!=null){
		   ////////System.out.println("tuple[index]::"+tuple[index]);
			if(initialMap.containsKey(tuple[index].toString())){
			List<Object[]> l=initialMap.get(tuple[index].toString());
			l.add(tuple);
			initialMap.put(tuple[index].toString(), l);
			
		}
			else{
				List<Object[]> l=new ArrayList<Object[]>();
				l.add(tuple);
				initialMap.put(tuple[index].toString(), l);
				
				
				
			}
			
			tuple=leftOper.readOneTuple();
			
		}
		
		
	}
	
	public <T> T[] concat(T[] first, T[] second) {
		////////System.out.println("first"+first.length+"second"+second.length);
		T[] result = Arrays.copyOf(first, first.length + second.length);
		System.arraycopy(second, 0, result, first.length, second.length);
	      
		for(int i=0;i<result.length;i++){
			////////System.out.println("joined "+result[i].toString());
		}
			return result;

		}

	@Override
	public Object[] readOneTuple() {
		if(index==list.size())
			return null;
		Object[] ret=list.get(index);
		index++;
		////////System.out.println("returning");
		return ret;
		
	}

	@Override
	public void reset() {
		
	}

	@Override
	public Datum[] getTuple() {
		
		return null;
	}
	
	private void buildHashMap(Column[] schema){
		int colIndex=0;
		for (Column col : schema) {
			////////System.out.println("vvvv");
			if (col.toString().contains(".")) {
				colIndexMap.put(col.toString(), colIndex);
				colIndexMap.put((col.toString()).split("\\.")[1], colIndex);
					}
			else {
				colIndexMap.put(col.toString(), colIndex);
			}
			colIndex++;
			col2Table.put(col.getColumnName(), col.getTable().getName());
		}
		
		
		
		
	}
	
	public Expression buildQuery(String key){
       // //////System.out.println("inside build query");
		String query = "";
	    Expression exp=null;
	    List l=PlainSelectEvaluator.map.get(key);
		
	    if(l!=null){
	    for(int i=0;i<l.size();i++){
			////////System.out.println("inside the loop"+l.get(i));
	    	if(i>0){
				query = query + " " + "and" +" ";
			}
			query = query+l.get(i);
		}				
		
		String sqlQuery = "select * from "+key+" where "+query+";";
		////////System.out.println("query is"+sqlQuery);
		CCJSqlParser tempParser = new CCJSqlParser(new StringReader(sqlQuery));
		Statement stmt = null;
		try {
			stmt = tempParser.Statement();
		} catch (ParseException e) {
			e.printStackTrace();
		}
	    if(stmt instanceof Select){
	    	SelectBody select = ((Select)stmt).getSelectBody();
	    	if(select instanceof PlainSelect){
	    	PlainSelect pselect=(PlainSelect)select;	
	    	exp=pselect.getWhere();
	    	}
	    }
		}
	return exp;
}

	}
