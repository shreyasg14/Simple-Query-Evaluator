package edu.buffalo.cse562;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;

import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.PlainSelect;

public class OrderByOperator implements Operator {

	Operator oper;
	Column[] schema;
	PlainSelect pselect;
	String column;
	List orderByList;
	String order;
	String columnName;
	int colIndex;
	ArrayList<Object[]> datumsList;
	Datum[][] datumsArray;
	public Iterator iterDatums;
	static int flag;
	int count;
	ArrayList<Object[]>  tempDatumList;
	public static LinkedHashMap<String,ArrayList<Object[]>> map=new LinkedHashMap<String, ArrayList<Object[]>>();
	ArrayList<Object[]> l;
    static int firstTime=0;
    public OrderByOperator(Operator oper, Column[] schema, String column,PlainSelect pselect) {
		//System.out.println("inside orderby");
    	this.oper = oper;
		this.schema = schema;
		this.column=column;
		this.pselect = pselect;
		datumsList = new ArrayList<Object[]>();
		//int i = 0;
		
		/*for(Column c:schema){
			//System.out.println(c.getColumnName());
			
		}*/
		Object[] d = null;
		
			////////System.out.println("flagAGG");
			
			if(column.toString().trim().contains(" ")){
				columnName = column.split(" ")[0];
				order=column.split(" ")[1];
			}
			
			else{
			columnName=column;
			//columnName = orderByList.get(i).toString();
			order="ASC";
			}
			
			
			if(PlainSelectEvaluator.aliasToColumn.containsKey(columnName)){
				//////System.out.println("matched");
		           for(int count=0;count<pselect.getSelectItems().size();count++){
		    	    if(pselect.getSelectItems().get(count).toString().contains("AS")){
		    		   if(PlainSelectEvaluator.aliasToColumn.containsKey(pselect.getSelectItems().get(count).toString().split("AS")[1].trim())){
		    			   colIndex=count;
		    			   OrderByOperator.flag=1;  
		    			  //System.out.println("matched key"+colIndex);
		    		   }
		    	   }
		       }
			}
			else{
			
			colIndex = getColumnIndex4mSchema(columnName);
			}
			
			if(firstTime==0){
			  d = oper.readOneTuple();
				while (d != null) {
					datumsList.add(d);
					d = oper.readOneTuple();
				}
			}	
				sortTable(colIndex);
				
				//System.out.println("sorted table");
				/*iterDatums = datumsList.iterator();
				while(iterDatums.hasNext()){
				Object[] ret=(Object[])iterDatums.next();
				//System.out.print(ret[0].toString()+ret[1].toString()+ret[2].toString());
				}*/
				
				
				if(firstTime==0){
				firstTime++;
				iterDatums = datumsList.iterator();
				while(iterDatums.hasNext()){
				Object[] ret=(Object[])iterDatums.next();
				String  key=ret[colIndex].toString();
				////System.out.println("map key:"+key);
				if(!map.isEmpty()){
					////System.out.println("key in empty loop"+key);
					if(!map.containsKey(key)){
						count=0;
					}
				}
				
				if(count==0){
					////System.out.println("key in count loop"+key);
					l=new ArrayList<Object[]>();
					l.add(ret);
					map.put(key, l);
					count++;
				}
				
				else{
					////System.out.println("key in else loop"+key);
					l=map.get(key);
					l.add(ret);
					map.put(key, l);
				
			}}		
				}
			////System.out.println("datumList size"+datumsList.size());
			
			iterDatums = datumsList.iterator();
			}

	public Object[] readOneTuple() {
		while(iterDatums.hasNext()) {
		//////System.out.println("hello");
	
		Object[] ret=(Object[]) iterDatums.next();
		/*	////System.out.println(ret.length);
			String  key=ret[colIndex].getValue().toString();
			////System.out.println("check");
			if(!map.isEmpty()){
				////System.out.println("key in empty loop"+key);
				if(map.containsKey(key)){
					count=0;
				}
			}
			
			if(count==0){
				////System.out.println("key in count loop"+key);
				l=new ArrayList<Datum[]>();
				l.add(ret);
				map.put(key, l);
				count++;
			}
			
			else{
				////System.out.println("key in else loop"+key);
				l=map.get(key);
				l.add(ret);
				map.put(key, l);
			}*/
			return ret;
		}

		return null;
	}

	public void sortTable(int colIndex) {
	    if(firstTime==0){
	    if(OrderByOperator.flag==1)
	    	Collections.sort(datumsList, new DatumComparator(colIndex,schema,order,"double"));	
		
	    else{
	    	Collections.sort(datumsList, new DatumComparator(colIndex,schema,order,FromScanner.tableMeta.get(schema[colIndex].getTable().getName()+"|"+schema[colIndex].getColumnName())));
            //System.out.println("datatype"+FromScanner.tableMeta.get(schema[colIndex].getTable().getName()+"|"+schema[colIndex].getColumnName()));    
	    }////System.out.println("colindex"+colIndex);
	    }
	    else{
	    	////System.out.println("inside else");
	    	datumsList=new ArrayList<Object[]>();
	    	
	    	Set<String> keySet=map.keySet();
	    	////System.out.println("keyset size"+map.keySet().size());
	        Iterator it=keySet.iterator();
	        while(it.hasNext()){
	        	String key=it.next().toString();
	        	////System.out.println("key::"+key);
	        	if(map.get(key).size()>1){
	        		tempDatumList= new ArrayList<Object[]>();
	        		for(int i=0;i<map.get(key).size();i++){
	        		tempDatumList.add(map.get(key).get(i));	
	        		}
	        		Collections.sort(tempDatumList, new DatumComparator(colIndex,schema,order,FromScanner.tableMeta.get(schema[colIndex].getTable().getName()+"|"+schema[colIndex].getColumnName())));
	        	    for(int j=0;j<tempDatumList.size();j++){
	        	    	datumsList.add(tempDatumList.get(j));
	        	    	
	        	    }
	        	    ////System.out.println("size of datum list in if"+datumsList.size());
	        	}
	        	else{
	        		
	        	datumsList.add(map.get(key).get(0));
	        	////System.out.println("size of datum list in else"+datumsList.size());
	        	}
	        	
	        }
	        iterDatums=datumsList.iterator();
	        //////System.out.println("datums size"+datumsList.size());
	    }
	    OrderByOperator.flag=0;
	}

	public void reset() {

	}

	public Datum[] getTuple() {
		return null;
	}

	public int getColumnIndex4mSchema(String colName) {
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
			//	//////System.out.println("col:"+col.getColumnName());
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
