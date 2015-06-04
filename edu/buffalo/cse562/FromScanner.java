package edu.buffalo.cse562;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;

import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.FromItemVisitor;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;

public class FromScanner implements FromItemVisitor{

	File basePath;
	
	HashMap<String,CreateTable> tables;
	public static Column[] schema;
	public Operator source;
	public static HashMap<String,String> tableMeta=new HashMap<String, String>();
	public static int subcheck;
	public FromScanner(File basePath,HashMap<String,CreateTable> tables){
		this.basePath = basePath;
		this.tables = tables;
		
	}

	public void visit(Table tableName) {
		//System.out.println("table visit");
		String oldTableName=tableName.getName();
		String alias;
		
		if(tableName.getAlias()!=null){
			//System.out.println("table has an alias");
			CreateTable ct1=tables.get(tableName.getName());
            ////////System.out.println("ct1"+ct1.toString());			
			alias=tableName.getAlias();
			tableName.setName(alias);
			tables.put(alias, ct1);
		////////System.out.println("alias"+alias);
			
		}
		
		String tName=tableName.getName();
		ColumnDefinition col=null;
		CreateTable ct = tables.get(tableName.getName());
		if(ct==null)
		{
			ct = tables.get(tableName.getName().toUpperCase());
		}
		if(ct!=null){
		//////System.out.println("check");
		List cols = ct.getColumnDefinitions();
		schema = new Column[cols.size()];
		////////System.out.println("Hello");
		for(int colCount = 0; colCount<cols.size(); colCount++){
			 col= (ColumnDefinition)cols.get(colCount);
			schema[colCount] = new Column(tableName,col.getColumnName()); 
			//////System.out.println("schema:"+schema[colCount].getColumnName());
			tableMeta.put(tName+"|"+schema[colCount].getColumnName(), col.getColDataType().toString());
		}
		}
		//////System.out.println(oldTableName);
		//System.out.println("calling scan operator");
		/*for(Column c:schema){
			//System.out.println("column:"+c.toString());
		}*/
		//System.out.println("old table:"+oldTableName.toString());
		if(new File(basePath,oldTableName+".dat").exists())
			try {
			source =new ScanOperator(new File(basePath,oldTableName+".dat"),tableMeta,schema,tableName);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		else
			try {
			source=	new ScanOperator(new File(basePath,oldTableName.toUpperCase()+".dat"),tableMeta,schema,tableName);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}	
		//source.reset();	
	
				
	}
	
	public Column[] getSchema(){
		return schema;
	}

	public void visit(SubSelect sub) {
		// TODO Auto-generated method stub
		subcheck = 1;
		SelectBody s =null;
		////System.out.println("subselect");
		s = (SelectBody)sub.getSelectBody();
		if(s instanceof PlainSelect){
			PlainSelect pselect = (PlainSelect)s;
			////////System.out.println("s:"+s.toString());
			PlainSelectEvaluator evaluator=new PlainSelectEvaluator(basePath,tables);
			//evaluator.processSelect();
			s.accept(evaluator);
			//System.out.println("after process");
			//schema = evaluator.leftSchema;
			//schema = null;
			//if(ProjectionOperator.newSchema!=null)
			//OperatorTest.dump(evaluator.oper);
			schema=new Column[evaluator.tempSchema.length];
			schema = evaluator.tempSchema;
			/*for(Column c:schema){
				//System.out.println("********");
				//System.out.println(c.toString()+" :column name ");
			}*/
			String[] dataType = new String[schema.length];
			
			int i = 0;
			for(Column c:schema){
              //////System.out.print(c.toString()+"\t");
				dataType[i++] = tableMeta.get(c.getTable().getName()+"|"+c.getColumnName());
			}
//			////////System.out.println("");
			
			if (sub.getAlias() != null) {
				Table table = new Table();
				table.setName(sub.getAlias());
				for (int j = 0; j < schema.length; j++) {
					schema[j].setTable(table);
					////////System.out.println("Check Table Meta Plain Select:"+schema[j].getTable().getName()+"|"+schema[j].getColumnName());
					tableMeta.put(schema[j].getTable().getName()+"|"+schema[j].getColumnName(), dataType[j]);
					
				}
			}
			source = evaluator.oper;
			////System.out.println("dumping");
			//OperatorTest.dump(source);
			source.reset();
			////System.out.println("end subselect");
		}
	}

	public void visit(SubJoin arg0) {
		// TODO Auto-generated method stub
		
	}
	
}
