package edu.buffalo.cse562;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

public class ScanOperator implements Operator{
	
	BufferedReader input;
	FileInputStream fis;
	File f;
	public static HashMap<String,String> tableMeta;
	ColumnDefinition col;
	Column[] schema;
	Table tableName;
	static int count =0;
	static int k=0;
	static List<Object> values=new ArrayList<Object>();
	//static String values[];
	public ScanOperator(File f,HashMap<String,String> tableMeta,Column[] schema,Table tableName) throws IOException{
		this.f = f;
		ScanOperator.tableMeta=tableMeta;
		this.schema=schema;
		this.tableName=tableName;
		reset();
	    
		String eachRecord="";
		//createBuffer();
		/*while((eachRecord=input.readLine())!=null){
	    	count++;
	    	//System.out.println("count"+count);
	    	values.add(eachRecord);
	    	
	    }*/
	
	}
	
	
	
	public Object[] readOneTuple(){
		//System.out.println("k"+k);
		String s="";
		try {
			while((s=input.readLine())!=null ){			
			    Object[] obj=s.split("\\|");
				return obj;	
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return null;
	  /* if(k<values.size()){
		  // System.out.println("returning");
		   if(values.get(k)==null){
			   return null;
		   }
		   
		   String s=values.get(k).toString();
		   Object[] obj=s.split("\\|");
		   
		   k++;
		   return obj;
	   }
	   
	   else{
		   
		   k=1;
		   values=new ArrayList<Object>();
		   try {
			createBuffer();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		   if(values.size()==0){
			   return null;
		   }
		   String s=values.get(0).toString();	   
		   Object[] obj=s.split("\\|");
		   return obj;
	   }*/
	   
	   
	   
	   
	   
		
		
		
		
	}
	private void createBuffer() throws IOException {
		String s="";
		int i=2;
		while((s=input.readLine())!=null && i>0){			
			values.add(s);
			i--;
			
		}
		if(s!=null){
		values.add(s);
		}
	}



	public void reset(){
		try {
			input=new BufferedReader(new FileReader(f));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	/*public void reset(){
		try{
		input = new BufferedReader(new FileReader(f));
		}
		catch(IOException e){
			e.printStackTrace();
		}
	}
*/
	public Datum[] getTuple() {
		// TODO Auto-generated method stub
		return null;
	}

}
	