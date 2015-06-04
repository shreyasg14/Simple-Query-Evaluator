package edu.buffalo.cse562;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Comparator;
import java.util.Date;

import net.sf.jsqlparser.schema.Column;

public class DatumComparator implements Comparator<Object[]> {
   int colIndex;
   Column[] schema;
   static String order="ASC";
   static String dataType=null;
   public DatumComparator(int colIndex,Column[] schema, String order,String dType){
		this.colIndex=colIndex;
        this.schema=schema;
        DatumComparator.dataType=dType;
        DatumComparator.order=order;
	}
	
	public int compare(Object[] d1, Object[] d2){
		if(OrderByOperator.flag==1){
			////System.out.println("flag==1");
			int ret=compare(d1[colIndex],d2[colIndex], -1,schema);
			 return ret;
		}
		
		else{
			//System.out.println("ffff");
			////System.out.println("COLiNDE"+d1[colIndex].getValue().toString());
			return compare(d1[colIndex],d2[colIndex], colIndex,schema);
		}
		// TODO Auto-generated method stub
		
	}
	
	public int compare(Object d1,Object d2,int index,Column[] schema) {
		
		if(DatumComparator.order.equals("DESC")){
			//System.out.println("inside descending");
			if(dataType!=null){
				//System.out.println("");
				if(dataType.contains("int") || dataType.contains("INT")){
					if((Integer)(d1)<((Integer)(d2)))
							{
						       return 1;
							}
					
					else if((Integer)(d1)==((Integer)(d2))){
						return 0;
					}
					
					else
						return -1;
				}
				
				
				if(dataType.contains("double")){
					
					
					
					if((Double)(d1)<((Double)(d2)))
							{
						       return 1;
							}
					
					else if((Double)(d1)==((Double)(d2))){
						return 0;
					}
					
					else
						return -1;
				}
				
				else if(dataType.contains("string")||dataType.contains("char")||dataType.contains("CHAR")||
						dataType.contains("VARCHAR")||dataType.contains("varchar")){
					System.out.println("inside varchar");
					//System.out.println("first: "+d1().toString()+"second:"+d2.getValue().toString().replaceAll("'", "").trim());
					int compareVal = d1.toString().compareTo(d2.toString().replaceAll("'", "").trim()); 
					/*//System.out.println("first "+d1.getValue().toString()+"second "+ d2.getValue().toString().replaceAll("'", "").trim()+"compare"+compareVal);
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
					if((Double)(d1)<((Double)(d2)))
						{
						return 1;
					}
					else if((Double)(d1)==((Double)(d2))){
						return 0;
					}
					
					else
						return -1;
					
				}
				//else if(dateCheck){
			else if(dataType.contains("date")||dataType.contains("DATE")){
				////System.out.println("BC compare kar");	
				SimpleDateFormat date=new SimpleDateFormat("yyyy-MM-dd");
					Date date1 = null,date2 = null;
					String str1 = null,str2= null;
					try {
						if(d2.toString().contains("'")){
							str1 = d1.toString();
							str2 = d2.toString().split("\\'")[1];
						}
						else if(d1.toString().contains("'")){	
							str1 = d1.toString().split("\\'")[1];
							str2 = d2.toString();
						}
						date1=date.parse(str1);
						date2 = date.parse(str2);
						if((date1.compareTo(date2))>0){
							return -1;
						}
						
						else if((date1.compareTo(date2))==0){
							return 0;
						}
						
						else{
							////System.out.println("date1"+date1.toString()+"::"+date2.toString());
							return 1;
						}
						
					} catch (ParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
			else{
				////System.out.println("first :"+d1.getValue().toString()+ "second "+d2.getValue().toString());
				if((Double)(d1)<((Double)(d2)))
				{
				return 1;
			}
			else if((Double)(d1)==((Double)(d2))){
				return 0;
			}
			
			else
				return -1;	
				}
			
		}
		
		else{
		if(dataType!=null){
		if(dataType.contains("int") || dataType.contains("INT")){
			if((Integer)(d1)<((Integer)(d2)))
					{
				       return -1;
					}
			
			else if((Integer)(d1)==((Integer)(d2))){
				return 0;
			}
			
			else
				return 1;
		}
		
		
		if(dataType.contains("double")){
			
			
			
			if((Double)(d1)<((Double)(d2)))
					{
				       return -1;
					}
			
			else if((Double)(d1)==((Double)(d2))){
				return 0;
			}
			
			else
				return 1;
		}
		
		else if(dataType.contains("string")||dataType.contains("char")||dataType.contains("CHAR")
				|| dataType.contains("VARCHAR")||dataType.contains("varchar")){
			//System.out.println("ascending");
			//System.out.println("first: "+d1.toString()+"second:"+d2.getValue().toString().replaceAll("'", "").trim());
			int compareVal = d1.toString().compareTo(d2.toString().replaceAll("'", "").trim()); 
			//System.out.println("first "+d1.toString()+"second "+ d2.toString().replaceAll("'", "").trim()+"compare"+compareVal);
			
			if( compareVal < 0 )
			{
			return -1;
		}
		 else if(compareVal == 0){
			return 0;
		}
		
		else
			return 1;
		}
		
		else if(dataType.contains("float")||dataType.contains("decimal")){
			if(Float.parseFloat(d1.toString())<(Float.parseFloat(d2.toString())))
				{
				return -1;
			}
			else if(Float.parseFloat(d1.toString())==(Float.parseFloat(d2.toString()))){
				return 0;
			}
			
			else
				return 1;
			
		}
		//else if(dateCheck){
	else if(dataType.contains("date")||dataType.contains("DATE")){
		////System.out.println("BC compare kar");	
		SimpleDateFormat date=new SimpleDateFormat("yyyy-MM-dd");
			Date date1 = null,date2 = null;
			String str1 = null,str2 = null;
			//System.out.println("d2"+d2.getValue().toString());
			//System.out.println("d1"+d1.getValue().toString());
			try {
				if(d2.toString().contains("'")){
					//System.out.println("check");
					str1 = d1.toString();
					str2 = d2.toString().split("\\'")[1];
				}
				else if(d1.toString().contains("'")){	
					//System.out.println("check1");
					str1 = d1.toString().split("\\'")[1];
					str2 = d2.toString();
				}
				else{
					str1 = d1.toString();
					str2 = d2.toString();
				}
				//System.out.println("date"+str1);
				date1=date.parse(str1);
				date2 = date.parse(str2);
				if((date1.compareTo(date2))>0){
					return 1;
				}
				
				else if((date1.compareTo(date2))==0){
					return 0;
				}
				
				else{
					////System.out.println("date1"+date1.toString()+"::"+date2.toString());
					return -1;
				}
				
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	else{
		if(Float.parseFloat(d1.toString())<(Float.parseFloat(d2.toString())))
		{
		return -1;
	}
	else if(Float.parseFloat(d1.toString())==(Float.parseFloat(d2.toString()))){
		return 0;
	}
	
	else
		return 1;	
	}
		}
		return 0;
	}
	
	
	
	
}
