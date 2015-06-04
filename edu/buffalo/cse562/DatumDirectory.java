package edu.buffalo.cse562;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import net.sf.jsqlparser.schema.Column;

public class DatumDirectory implements Datum {
	
	public static boolean intCheck=false;
	public static boolean stringCheck=false;
	public static boolean floatCheck=false;
	public static boolean doubleCheck=false;
	public static boolean dateCheck=false;
	public static boolean booleanCheck=false;
    
	Object value;
    public DatumDirectory(){
    	
    }
	public DatumDirectory(Object value){
    	this.value=value;
    }
	
	public Object getValue() {
		
		return value;
	}

	public int compare(Datum d,int index,Column[] schema) {
		//System.out.println("inside compare");
		//schema = FromScanner.schema;
		/*for(Column c:schema){
			////System.out.println("column:"+c.toString());
		}*/
		//String[] tableColumnName = schema[index].toString().split("\\.");
		//////System.out.println("key"+schema[index].getTable().getName()+"|"+schema[index].getColumnName());
		String dataType = null;
		////System.out.println("hello"+schema[index].getTable().getName()+"|"+schema[index].getColumnName());
		if(index!=-1){
		 dataType=FromScanner.tableMeta.get(schema[index].getTable().getName()+"|"+schema[index].getColumnName());
		}
		//System.out.println("dataType"+dataType);
		if(DatumComparator.order.equals("DESC")){
			////System.out.println("inside descending");
			if(dataType!=null){
				////System.out.println("");
				if(dataType.contains("int") || dataType.contains("INT")){
					if(Integer.parseInt(this.value.toString())<(Integer.parseInt(d.getValue().toString())))
							{
						       return 1;
							}
					
					else if(Integer.parseInt(this.value.toString())==(Integer.parseInt(d.getValue().toString()))){
						return 0;
					}
					
					else
						return -1;
				}
				
				
				if(dataType.contains("double")){
					
					
					
					if(Double.parseDouble(this.value.toString())<(Double.parseDouble(d.getValue().toString())))
							{
						       return 1;
							}
					
					else if(Double.parseDouble(this.value.toString())==(Double.parseDouble(d.getValue().toString()))){
						return 0;
					}
					
					else
						return -1;
				}
				
				else if(dataType.contains("string")||dataType.contains("STRING")||dataType.contains("char")||dataType.contains("CHAR")){
					/*//System.out.println("here");
					//System.out.println("first: "+this.value.toString()+"second:"+d.getValue().toString().replaceAll("'", "").trim());
					*/
					int compareVal = this.value.toString().compareTo(d.getValue().toString().replaceAll("'", "").trim()); 
					/*//System.out.println("first "+this.value.toString()+"second "+ d.getValue().toString().replaceAll("'", "").trim()+"compare"+compareVal);
					*/
					if( compareVal < -1 )
					{
					return 1;
				}
				else if(compareVal == 0){
					return 0;
				}
				
				else
					return -1;
				}
				
				else if(dataType.contains("float")||dataType.contains("decimal")){
					if(Float.parseFloat(this.value.toString())<(Float.parseFloat(d.getValue().toString())))
						{
						return 1;
					}
					else if(Float.parseFloat(this.value.toString())==(Float.parseFloat(d.getValue().toString()))){
						return 0;
					}
					
					else
						return -1;
					
				}
				//else if(dateCheck){
			else if(dataType.contains("date")||dataType.contains("DATE")){
				//////System.out.println("BC compare kar");	
				SimpleDateFormat date=new SimpleDateFormat("yyyy-MM-dd");
					Date date1 = null,date2 = null;
					String d1 = null,d2 = null;
					////System.out.println("this.value.toString()"+this.value.toString());
					////System.out.println("d1"+d.getValue().toString());
					
					try {
						if(d.getValue().toString().contains("'")){
							d1 = this.value.toString();
							d2 = d.getValue().toString().split("\\'")[1];
						}
						else if(this.value.toString().contains("'")){	
							d1 = this.value.toString().split("\\'")[1];
							d2 = d.getValue().toString();
						}
						date1=date.parse(d1);
						date2 = date.parse(d2);
						if((date1.compareTo(date2))>0){
							return -1;
						}
						
						else if((date1.compareTo(date2))==0){
							return 0;
						}
						
						else{
							//////System.out.println("date1"+date1.toString()+"::"+date2.toString());
							return 1;
						}
						
					} catch (ParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
			else{
				////System.out.println("first :"+this.value.toString()+ "second "+d.getValue().toString());
				if(Float.parseFloat(this.value.toString())<(Float.parseFloat(d.getValue().toString())))
				{
				return 1;
			}
			else if(Float.parseFloat(this.value.toString())==(Float.parseFloat(d.getValue().toString()))){
				return 0;
			}
			
			else
				return -1;	
				}
			
		}
		
		else{
		if(dataType!=null){
		if(dataType.contains("int") || dataType.contains("INT")){
			if(Integer.parseInt(this.value.toString())<(Integer.parseInt(d.getValue().toString())))
					{
				       return -1;
					}
			
			else if(Integer.parseInt(this.value.toString())==(Integer.parseInt(d.getValue().toString()))){
				return 0;
			}
			
			else
				return 1;
		}
		
		
		if(dataType.contains("double")){
			
			
			
			if(Double.parseDouble(this.value.toString())<(Double.parseDouble(d.getValue().toString())))
					{
				       return -1;
					}
			
			else if(Double.parseDouble(this.value.toString())==(Double.parseDouble(d.getValue().toString()))){
				return 0;
			}
			
			else
				return 1;
		}
		
		else if(dataType.contains("string")||dataType.contains("char")||dataType.contains("CHAR")){
			////System.out.println("first: "+this.value.toString()+"second:"+d.getValue().toString().replaceAll("'", "").trim());
			int compareVal = this.value.toString().compareTo(d.getValue().toString().replaceAll("'", "").trim()); 
			////System.out.println("first "+this.value.toString()+"second "+ d.getValue().toString().replaceAll("'", "").trim()+"compare"+compareVal);
			
			if( compareVal < -1 )
			{
			return -1;
		}
		else if(compareVal == 0){
			return 0;
		}
		
		else
			return 1;
		}
		
		else if(dataType.contains("float")||dataType.contains("decimal")||dataType.contains("DECIMAL")){
			//System.out.println("check");
			if(Float.parseFloat(this.value.toString())<(Float.parseFloat(d.getValue().toString())))
				{
				return -1;
			}
			else if(Float.parseFloat(this.value.toString())==(Float.parseFloat(d.getValue().toString()))){
				return 0;
			}
			
			else
				return 1;
			
		}
		//else if(dateCheck){
	else if(dataType.contains("date")||dataType.contains("DATE")){
		//System.out.println("BC compare kar");	
		SimpleDateFormat date=new SimpleDateFormat("yyyy-MM-dd");
			Date date1 = null,date2 = null;
			String d1 = null,d2 = null;
			try {
				Date t = date.parse("1999-10-01");
				
				if(d.getValue().toString().contains("'")){
					//System.out.println("check");
					d1 = this.value.toString();
					d2 = d.getValue().toString().replaceAll("'","");
				}
				if(this.value.toString().contains("'")){
					//System.out.println("check1");
					d1 = this.value.toString().replaceAll("'","");
					d2 = d.getValue().toString();
				}
				date1=date.parse(d1);
				date2 = date.parse(d2);
				//System.out.println("check "+d1.toString()+"d2 "+d2.toString()+"compare"+date1.compareTo(date2));
				
				//System.out.println("date1:"+date1+"date2"+date2+"compare"+date1.before(date2));
				if(date1.compareTo(date2)>0){
					//System.out.println("return 1");
					return 1;
				}
				
				else if((date1.compareTo(date2))==0){
					//System.out.println("return 0");
					return 0;
				}
				
				else if((date1.compareTo(date2))<0){
					//System.out.println("return -1");
					return -1;
				}
				
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	else{
		if(Float.parseFloat(this.value.toString())<(Float.parseFloat(d.getValue().toString())))
		{
		return -1;
	}
	else if(Float.parseFloat(this.value.toString())==(Float.parseFloat(d.getValue().toString()))){
		return 0;
	}
	
	else
		return 1;	
	}
		}
		return 0;
	}
	
	
		
	public Object addition(Datum d, int index) throws ClassCastException {
		Column[] schema = FromScanner.schema;
		//Changes made by Subhadeep
		String[] tableColumnName = schema[index].toString().split("\\.");
		//Changes made by Subhadeep
		String dataType = FromScanner.tableMeta.get(tableColumnName[0]+"|"+tableColumnName[1]);
		//dataType.contains("date")
		//////System.out.println("INdex :"+index);
		if(dataType.contains("int")){
		
			
			return ( Integer.parseInt(this.value.toString())+(Integer.parseInt(d.getValue().toString())) );
		
		}
		
		
		if(dataType.contains("double")){
			
			return Double.parseDouble(this.value.toString())+(Double.parseDouble(d.getValue().toString()));
				
		}
		
		else if(dataType.contains("string")){
			return 0;
		}
		
		else if(dataType.toLowerCase().contains("float")||dataType.toLowerCase().contains("decimal")){
			return Float.parseFloat(this.value.toString())+(Float.parseFloat(d.getValue().toString()));
		}
		else if(dataType.contains("date")){
			return 0;
		}
		return 0;
	}
	
public Object subtraction(Datum d, int index) throws ClassCastException {
	//Changes made by Subhadeep
			Column[] schema = FromScanner.schema;
			//Changes made by Subhadeep
			String[] tableColumnName = schema[index].toString().split("\\.");
			//Changes made by Subhadeep
			String dataType = FromScanner.tableMeta.get(tableColumnName[0]+"|"+tableColumnName[1]);
			//Changes made by Subhadeep
			//////System.out.println("tableColumnName[0]"+tableColumnName[0]+"tableColumnName[1]"+tableColumnName[1]);
			//////System.out.println("datatype"+dataType);
		
		if(dataType.contains("int")||dataType.contains("INT")){
		
			
			return ( Integer.parseInt(this.value.toString())-(Integer.parseInt(d.getValue().toString())) );
		
		}
		
		
		if(dataType.contains("double")||dataType.contains("DOUBLE")){
			
			return Double.parseDouble(this.value.toString())-(Double.parseDouble(d.getValue().toString()));
				
		}
		
		else if(dataType.contains("string")){
			return 0;
		}
		
		else if(dataType.contains("float")||dataType.contains("decimal")||dataType.contains("FLOAT")||dataType.contains("DECIMAL")){
			return Float.parseFloat(this.value.toString())-(Float.parseFloat(d.getValue().toString()));
		}
		else if(dataType.contains("date")){
			return 0;
		}
		return 0;
	}

public Object multiplication(Datum d,int index) throws ClassCastException {
	//Changes made by Subhadeep
			Column[] schema = FromScanner.schema;
			//Changes made by Subhadeep
			String[] tableColumnName = schema[index].toString().split("\\.");
			//Changes made by Subhadeep
			String dataType = ScanOperator.tableMeta.get(tableColumnName[0]+"|"+tableColumnName[1]);
			//Changes made by Subhadeep
			
	
	if(dataType.contains("int") || dataType.contains("INT")){
	
		
		return ( Integer.parseInt(this.value.toString())*(Integer.parseInt(d.getValue().toString())) );
	
	}
	
	
	if(dataType.contains("double")||dataType.contains("DOUBLE")){
		
		return Double.parseDouble(this.value.toString())*(Double.parseDouble(d.getValue().toString()));
			
	}
	
	else if(dataType.contains("string")){
		return 0;
	}
	
	else if(dataType.contains("float")||dataType.contains("decimal")||dataType.contains("DECIMAL")||dataType.contains("FLOAT")){
		return Float.parseFloat(this.value.toString())*(Float.parseFloat(d.getValue().toString()));
	}
	else if(dataType.contains("date")){
		return 0;
	}
	return 0;
}
	
public Object division(Datum d,int index) throws ClassCastException {
	//Changes made by Subhadeep
			Column[] schema = FromScanner.schema;
			//Changes made by Subhadeep
			String[] tableColumnName = schema[index].toString().split("\\.");
			//Changes made by Subhadeep
			String dataType = ScanOperator.tableMeta.get(tableColumnName[0]+"|"+tableColumnName[1]);
			//Changes made by Subhadeep
			
	
	if(dataType.contains("int")){
	
		
		return ( Integer.parseInt(this.value.toString())/(Integer.parseInt(d.getValue().toString())) );
	
	}
	
	
	if(dataType.contains("double")){
		
		return Double.parseDouble(this.value.toString())/(Double.parseDouble(d.getValue().toString()));
			
	}
	
	else if(dataType.contains("string")){
		return 0;
	}
	
	else if(dataType.contains("float")||dataType.contains("decimal")){
		return Float.parseFloat(this.value.toString())/(Float.parseFloat(d.getValue().toString()));
	}
	else if(dataType.contains("date")){
		return 0;
	}
	return 0;
}

	
	

}
