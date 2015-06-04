package edu.buffalo.cse562;

import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.PlainSelect;

public class ProjectionOperator implements Operator {

	Operator oper;
	Column[] schema;
	public static Column[] newSchema;
	PlainSelect pselect;
	int[] colSeq= null;
	public ProjectionOperator(Operator oper, Column[] schema,
			PlainSelect pselect) {
		//System.out.println("select items"+pselect.getFromItem().toString());
		this.oper = oper;
		this.schema = schema;
		this.pselect = pselect;
		colSeq = new int[schema.length];
		newSchema = new Column[schema.length];
		
		/*for(Column c:schema){
			
			////System.out.print("asdad"+c.toString()+"\n");
			////System.out.print(c.getTable().getName()+"\t");
		
		}*/
		//System.out.println(""); 	
		for(int pselectIndex = 0; pselectIndex < pselect.getSelectItems().size(); pselectIndex++){
			//System.out.println("value:"+pselect.getSelectItems().get(pselectIndex).toString());
			if(pselect.getSelectItems().get(pselectIndex).toString().contains(" AS ")){
				String alias = pselect.getSelectItems().get(pselectIndex).toString().split(" AS ")[1].trim();
				//System.out.println("Alias"+alias);
				colSeq[pselectIndex] = getColumnIndex4mSchema(alias);
				//System.out.println("index"+colSeq[pselectIndex]);
				
			}
			else{
				////////System.out.println("Projection error:"+pselect.getSelectItems().get(pselectIndex).toString());
				
			colSeq[pselectIndex] = getColumnIndex4mSchema(pselect.getSelectItems().get(pselectIndex).toString());
			////System.out.println("Columfsdjfkfjdgdkg:"+colSeq[pselectIndex] );
			}
			//System.out.println("Check:"+colSeq[pselectIndex]);
			//////System.out.println("pselect items:"+pselect.getSelectItems().get(pselectIndex).toString());
			//for(Column c:schema)
				////////System.out.println("schema:"c.getColumnName());
			newSchema[pselectIndex] = schema[colSeq[pselectIndex]];
			////////System.out.print(pselect.getSelectItems().get(pselectIndex).toString()+"\t");
			////////System.out.print(newSchema[pselectIndex].toString()+"\t");
			
		}
		
		////////System.out.println("");
	}

	public Object[] readOneTuple() {
		
		Object[] tuple = null;
		Object[] projTuple=null;
		
		if(PlainSelectEvaluator.scheck!=1){
		tuple=oper.readOneTuple();
		if(tuple!=null){
			projTuple = new Object[pselect.getSelectItems().size()];
			int size = pselect.getSelectItems().size();
			for(int tupIndex = 0; tupIndex < size; tupIndex++ ){
				//System.out.println("ds"+tuple.length);
				projTuple[tupIndex] = tuple[colSeq[tupIndex]];
			}
		}
		else{
			projTuple = null;
		}
		
		}

		
		else{
			tuple=oper.readOneTuple();
			return tuple;
		}
		return projTuple;
	}

	public void reset() {
		oper.reset();
	}

	public int getColumnIndex4mSchema(String colName) {
		 int colIndex = 0;
		
			//System.out.println("inside if");
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

	public Datum[] getTuple() {
		return null;
	}

}
