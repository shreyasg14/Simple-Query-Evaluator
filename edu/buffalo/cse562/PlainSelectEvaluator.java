package edu.buffalo.cse562;

import java.io.File;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectVisitor;
import net.sf.jsqlparser.statement.select.Union;

public class PlainSelectEvaluator implements SelectVisitor {

	// PlainSelect pselect;
	File dataDir;
	static HashMap<String, String> col2Table = new HashMap<String, String>();
	public static HashMap<String, Integer> colIndexMapforFirst = new HashMap<String,Integer>();
	public static HashMap<String, Integer> colIndexMap = new HashMap<String,Integer>();
	HashMap<String, CreateTable> sqlTables;
	Operator oper;
	public static List<String> tables = new ArrayList<String>();
	public static int check;
	public Column[] leftSchema;
	public Column[] tempSchema;
	public static int scheck;
	static long rowCount;
	// List<ArrayList> whereList=new ArrayList<ArrayList>();
	static HashMap<String, List> map = new HashMap<String, List>();

	public static HashMap<String, String> aliasToColumn = new HashMap<String, String>();
	int colIndex = 0;
	int unionCheck = 0;
	int expCheck = 0;
	int[] whereIndex;
	public static HashMap<String,String> joinMap=new HashMap<String, String>();
	List<String> l = new ArrayList<String>();
	int index = 0;
	List list;
	static int selExpCount = 0;
	String[] selString;

	public PlainSelectEvaluator(File dataDir,
			HashMap<String, CreateTable> sqlTables) {

		// this.pselect = pselect;
		this.dataDir = dataDir;
		this.sqlTables = sqlTables;
	}

	/*
	 * public void processSelect() {
	 * 
	 * }
	 */
	public Operator delColumns() {

		return oper;

	}

	public void visit(PlainSelect pselect) {
		selString = new String[pselect.getSelectItems().size()];
		// //////////System.out.println("plain select evaluator");
		FromScanner fromScan = new FromScanner(dataDir, sqlTables);
		FromItem fi[] = null;
		Expression expCheck2 = null;
		if (pselect.getFromItem().getAlias() != null) {
			tables.add(pselect.getFromItem().getAlias());
		} else {
			tables.add(pselect.getFromItem().toString());
		}

		Join joins = new Join();
		FromItem tempFromItem;
		int tableIndex;
		HashMap<String, Integer> hashTableSchemaLength = new HashMap<String, Integer>();
		String[] tableNamesOn = new String[2];
		if (pselect.getLimit() != null) {
			rowCount = pselect.getLimit().getRowCount();
		}
		
		if (pselect.getJoins() != null) {
			list = pselect.getJoins();
			Join j = new Join();
			for (int index = 0; index < list.size(); index++) {

				j = (Join) list.get(index);
				if (j.getRightItem().getAlias() != null)

				{
					tables.add(j.getRightItem().getAlias());
				} else {
					tables.add(list.get(index).toString());
				}
			}

		}

		
		
	
		String[] whereClauses=null;;
		if (pselect.getWhere() != null && pselect.getJoins() != null) {

			String where = pselect.getWhere().toString();
			

			if (where.contains("AND")) {

				// ////System.out.println("AND");

				whereClauses = where.split("AND");

				whereIndex = new int[whereClauses.length];
				String[] splitOnEquals;
				for (int i = 0; i < whereClauses.length; i++) {
					splitOnEquals = whereClauses[i].trim().split(" ");
					// ////System.out.println("splitOnEquals[2]::"+splitOnEquals[2]);
					if(whereClauses[i].contains("OR")){
					    //System.out.println("OR found"+ i);
						String key=whereClauses[i].split("OR")[1].trim().split("=")[0].split("\\.")[0].trim();
						for(int j=i;j<whereClauses.length;j++){
						  List list=map.get(key);
						  if(list==null){
							  list=new ArrayList<String>();
							  list.add(whereClauses[j]);
							  map.put(key, list);
						  }
						  
						  else{
							  list.add(whereClauses[j]);
							  map.put(key, list);
							  
						  }
					  }	
						break;
					}
					
					
					if (!splitOnEquals[2].contains(".")) {

						String tableName = splitOnEquals[0].split("\\.")[0];
						List a = map.get(tableName);
						if (a == null) {
							a = new ArrayList();
							a.add(whereClauses[i]);

						} else {
							
							a.add(whereClauses[i]);

						}
						
						map.put(tableName, a);
						whereIndex[index++] = i;
						// ////System.out.println("i"+ i);
					}
					
					

				}

			}

		}
		fi = new FromItem[tables.size()];
		
		fi[0] = pselect.getFromItem();
		

		
		if (pselect.getJoins() != null) {
			for (int b = 1; b <= list.size(); b++) {
				joins = (Join) list.get(b - 1);
				fi[b] = joins.getRightItem();
			}
			//System.out.println("where"+whereClauses.length);
			//New Join map
			if(pselect.getJoins()!=null){
			
			for(int i=0;i<fi.length-1;i++){				
				for(int j=0;j<whereClauses.length;j++){
					////System.out.println("fdf"+whereClauses[j].trim());
					if(whereClauses[j].trim().split(" ")[2].contains(".")){
						////System.out.println("i"+i);
					   // //System.out.println("where clauses"+whereClauses[j]);
						
						for(int k=i+1;k<fi.length;k++){
						String first=fi[i].toString();
						String second=fi[k].toString();
						String firstTerm=whereClauses[j].split("=")[0].split("\\.")[0].trim();
						if(!whereClauses[j].contains("=")){
							break;
						}
						String secondTerm=whereClauses[j].split("=")[1].split("\\.")[0].trim();
						String key=null;
						String value=null;
						
						if((first.equals(firstTerm) && second.equals(secondTerm)) ){
							key=first+"|"+second;
						   	value=whereClauses[j].trim();
						   	
						
						}
						
						else if((first.equals(secondTerm) && second.equals(firstTerm))){
							key=first+"|"+second;
							value=whereClauses[j].split("=")[1]+"="+whereClauses[j].split("=")[0];
							
						}
						
						if(key!=null){
							if(!joinMap.containsKey(key)){
								//System.out.println("key is " +key +" value is "+value );
								joinMap.put(key, value);
							}
						}
						}
						
					}
				
				
				
				}
				
				
			}
			}
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			/*int listLastIndex = list.size() - 1;
			if (((Join) list.get(listLastIndex)).getOnExpression() != null) {
				expCheck2 = ((Join) list.get(listLastIndex)).getOnExpression();

				JoinEvaluator evaluator = new JoinEvaluator();
				expCheck2.accept(evaluator);
				tableNamesOn[0] = evaluator.getLeftTableName();
				tableNamesOn[1] = evaluator.getRightTableName();
				
				tableIndex = tables.indexOf(tableNamesOn[0]);
				tempFromItem = fi[tableIndex];

				fi[tableIndex] = fi[0];
				fi[0] = tempFromItem;

				tableIndex = tables.indexOf(tableNamesOn[0]);
				tempFromItem = fi[tableIndex];

				fi[tableIndex] = fi[1];
				fi[1] = tempFromItem;*/

		}
		

		fi[0].accept(fromScan);
		leftSchema = FromScanner.schema;
		oper = fromScan.source;
		
		for (Column col : leftSchema) {
			if (col.toString().contains(".")) {
				colIndexMapforFirst.put(col.toString(), colIndex);
				colIndexMapforFirst.put((col.toString()).split("\\.")[1], colIndex);
					}
			else {
				colIndexMapforFirst.put(col.toString(), colIndex);
			}
			colIndex++;
		}
		colIndex=0;
		
		String tableName = leftSchema[0].getTable().getName();
		for (Column tempCol : leftSchema) {
			col2Table.put(tempCol.getColumnName(), tableName);
		}
		
		if(pselect.getJoins()!=null){
		Expression exp=buildQuery(fi[0].toString());
		////System.out.println("exp.to"+exp.toString());
		if(exp!=null){
			oper=new SelectionOperator(oper, leftSchema, exp, pselect.getSelectItems(), hashTableSchemaLength, fi,col2Table);
			//////System.out.println("expression" +exp.toString());
		}
		}
		else if(pselect.getWhere()!=null && pselect.getJoins()==null){
			
			oper=new SelectionOperator(oper, leftSchema, pselect.getWhere(), pselect.getSelectItems(), hashTableSchemaLength, fi,col2Table);
			
		}
		
		
		

		hashTableSchemaLength.put(fi[0].toString(), FromScanner.schema.length);
		
		List selectItems = pselect.getSelectItems();
		if (pselect.getWhere() != null) {
			check = 1;
		}

		Expression ex = null;
		
		
		
		if(pselect.getJoins()!=null){
			oper=new JoinTable(oper, leftSchema, fi, fromScan);
			leftSchema=JoinTable.joinedSchema;
		}
		
		
		for (Column col : leftSchema) {
			//////System.out.println("SChema:"+col.toString()+"\t"+pp++);
			if (col.toString().contains(".")) {
				colIndexMap.put(col.toString(), colIndex);
				//////System.out.println("after split:"+(col.toString()));
				colIndexMap.put((col.toString()).split("\\.")[1], colIndex);
				//////System.out.println("key:"+col.toString());
			}
			else {
				//////System.out.println("key:"+col.toString());
				colIndexMap.put(col.toString(), colIndex);
			}
			colIndex++;
		}
		
		
		if (selectItems.get(0).toString().equals("*")) {
			scheck = 1;
		}
		if (scheck != 1) {
		tempSchema = new Column[pselect.getSelectItems().size()];
		for (int selCount = 0; selCount < selectItems.size(); selCount++) {
			// expCheck=0;
			String str = selectItems.get(selCount).toString();
			// //////////System.out.println("string"+str+isAggregate(str));

			if (!isAggregate(str)) {
				if (isExpression(str)) {
					// //////////System.out.println("check");
					expCheck = 1;
				}
			}
			SelectExpressionItem se1 = (SelectExpressionItem) selectItems
					.get(selCount);

			if (se1.getAlias() != null) {
				// ////////System.out.println("check");
				// ////////System.out.println("se1.getExpression().toString()::"+se1.getExpression().toString());
				String str1 = se1.getExpression().toString();
				String s = se1.toString().split("AS")[0];
				// if(expCheck==1){
				// //////////System.out.println("string::"+str1);
				String str2 = null;
				if (str1.contains("\\+")) {
					str2 = str1.split("\\+")[0];
					str2 = str2.replaceAll("(", "");
				} else if (str1.contains("-")) {
					str2 = str1.split("-")[0];
					str2 = str2.replaceAll("\\(", "");
				} else if (str1.contains("\\+")) {
					str2 = str1.split("*")[0];
					str2 = str2.replaceAll("(", "");
				} else if (str1.contains("\\+")) {
					str2 = str1.split("\\+")[0];
					str2 = str2.replaceAll("(", "");
				}

				else {
					str2 = str1;
					if (str2.contains(".")) {
						// //////////System.out.println("lmlud");
						str2 = str2.split("\\.")[1];
					}

				}
				// //////////////System.out.println("str2:"+str2);
				for (Column c : leftSchema) {
					if (c.getColumnName().equals(str2.trim())) {
						Table table = new Table();
						if (expCheck == 1) {

							table.setName("C9");
						} else {
							table.setName(c.getTable().getName());
						}
						tempSchema[selCount] = new Column(table, se1.getAlias());
						FromScanner.tableMeta.put(
								"C9" + "|" + se1.getAlias(),
								FromScanner.tableMeta.get(c.getTable()
										.getName() + "|" + c.getColumnName()));
						// //////////System.out.println("key"+"C9"+"|"+se1.getAlias());
						break;
					}
				}

				// }

			} else if (str.contains(".")) {
				Table table = new Table();
				table.setName(str.split("\\.")[0]);
				tempSchema[selCount] = new Column(table, str.split("\\.")[1]);

			} else {
				for (Column c : leftSchema) {
					if (c.getColumnName().equals(str)) {
						Table table = new Table();
						table.setName(c.getTable().getName());
						tempSchema[selCount] = new Column(table, str);
						break;
					}
				}

			}

			
			
				SelectExpressionItem se = (SelectExpressionItem) selectItems
						.get(selCount);
				ex = se.getExpression();
				int colIndex = 0;
				// //////////////System.out.println("ex:"+ex.toString());
				// //////////////System.out.println("se:"+se.toString());
				if (isAggregate(ex.toString())) {

					// ////////////System.out.println("agg there!");
					if (se.getAlias() != null) {

						// //////////////////System.out.println("hello");
						aliasToColumn.put(se.getAlias(),
								se.toString().split("AS")[0].trim());
					}
				}

				// //////////////////System.out.println("value"+aliasToColumn.get(se.getAlias()));
				else if (isExpression(ex.toString())) {
					if (se.getAlias() != null) {
						selString[selExpCount++] = se.getAlias();
						// ////////////System.out.println("alias in exp");

					}

					else {
						// ////////////System.out.println("no alias in exp");
						selString[selExpCount++] = ex.toString();
					}

				}

				else {
					if (se.getAlias() != null) {
						// ////////////System.out.println("ex.to"+ex.toString());
						colIndex = getColumnIndex4mSchema(ex.toString());
						String tName = leftSchema[colIndex].getTable()
								.getName();
						String dType = FromScanner.tableMeta.get(tName + "|"
								+ leftSchema[colIndex].getColumnName());
						leftSchema[colIndex].setColumnName(se.getAlias());
						FromScanner.tableMeta.put(tName + "|"
								+ leftSchema[colIndex].getColumnName(), dType);
						FromScanner.schema = leftSchema;
					}
				}
			}
		}

		if (selExpCount > 0) {
			// ////////System.out.println("selexp >0");
			leftSchema = appendSchema();
		}

		/*if (pselect.getWhere() != null && pselect.getJoins()==null) {
			// //////////////////System.out.println("where");
			oper = new SelectionOperator(oper, leftSchema, pselect.getWhere(), pselect.getSelectItems(), hashTableSchemaLength, fi, col2Table);

		}
*/
		SelectItemsEvaluator eval = null;
		for (int sCount = 0; sCount < selectItems.size(); sCount++) {

			if (selectItems.get(0).toString().equals("*")) {
				scheck = 1;
			}
			if (scheck != 1) {
				int aliasIndex = 99;
				SelectExpressionItem se = (SelectExpressionItem) selectItems
						.get(sCount);
				// //////////System.out.println("select"+se.toString());
				// //////////System.out.println("****");
				if (se.getAlias() != null) {
					aliasIndex = getColumnIndex4mSchema(se.getAlias());
					if (isAggregate(se.toString())) {
						// ////////////System.out.println("matched");
						eval = new SelectItemsEvaluator(leftSchema, oper,
								pselect, se.getAlias(), null, aliasIndex);
						ex = se.getExpression();
						ex.accept(eval);

					}
				} else {
					if (isAggregate(se.toString())) {
						// //////////System.out.println("matched");
						eval = new SelectItemsEvaluator(leftSchema, oper,
								pselect, null, null, aliasIndex);
						ex = se.getExpression();
						ex.accept(eval);

					}

				}
				// if(expCheck==1){
				
				/* * eval = new SelectItemsEvaluator(leftSchema, oper,
				 * pselect,se.getAlias(), aliasIndex); ex = se.getExpression();
				 * ex.accept(eval);
				*/ 
				// //////////////System.out.println("check");
				for (int i = 0; i < selExpCount; i++) {
					// //////////System.out.println("value"+se.getExpression().toString());
					// //////////////System.out.println("value2:"+selString[i]);
					if (se.getAlias() != null) {
						if (se.getAlias().equals(selString[i])) {

							// //////System.out.println("inside ifff");
							oper = new SelectItemsOperator(oper, leftSchema,
									pselect, se, aliasIndex);
						}

					} else {
						if (se.getExpression().toString().equals(selString[i])) {

							// ////////////System.out.println("inside ifff!!!");
							// oper=new SelectItemsOperator(oper, leftSchema,
							// pselect,se, aliasIndex);
						}
					}
				}

			}
		}
		double cMemory = ((double) ((double) (Runtime.getRuntime()
				.totalMemory() / 1024) / 1024))
				- ((double) ((double) (Runtime.getRuntime().freeMemory() / 1024) / 1024));
		// ////System.out.println("after aggregate exec"+cMemory);

		if (SelectItemsEvaluator.aggCheck == 1) {
			// ////System.out.println("aggregate started");

			AggregateOperator2 aggregateOperator;
			aggregateOperator = new AggregateOperator2(oper, leftSchema,
					pselect);
			oper = aggregateOperator;
			// ////System.gc();
			// leftSchema = aggregateOperator.joinedSchema;
		}
		double currentMemory = ((double) ((double) (Runtime.getRuntime()
				.totalMemory() / 1024) / 1024))
				- ((double) ((double) (Runtime.getRuntime().freeMemory() / 1024) / 1024));
		long afterExec = Runtime.getRuntime().totalMemory();

		int count=0;
		for(int j=0;j<pselect.getSelectItems().size();j++){
			if(!isAggregate(pselect.getSelectItems().get(j).toString())){
				count++;
				
			}
		}
		//////System.out.println("count"+count);
		List l=pselect.getOrderByElements();
		
		if (pselect.getOrderByElements() != null) {
			
			if (pselect.getGroupByColumnReferences() == null) {
				for (int i = 0; i < pselect.getOrderByElements().size(); i++) {
					oper = new OrderByOperator(oper, leftSchema, pselect
							.getOrderByElements().get(i).toString(), pselect);
				}
			}

			else {
				Column[] tempSchema1 = new Column[pselect.getSelectItems().size()];
				for (int index = 0; index < pselect.getSelectItems().size(); index++) {
					//////System.out.println("here");
					if (isAggregate(pselect.getSelectItems().get(index)
							.toString())) {
						Table table=new Table();
						table.setName(" ");
						tempSchema1[index] = new Column(table,
								aliasToColumn
										.get(pselect.getSelectItems()
												.get(index).toString()
												.split("AS")[1].trim()));
						continue;
					}
					
					if (pselect.getSelectItems().get(index).toString()
							.contains("AS")) {
						if (aliasToColumn.containsKey(pselect.getSelectItems()
								.get(index).toString().split("AS")[1].trim())) {
							// //////////////System.out.println("check");
							Table table = new Table();
							table.setName("");
							// //////////////System.out.println("index1"+index);

							tempSchema1[index] = new Column(table,
									aliasToColumn
											.get(pselect.getSelectItems()
													.get(index).toString()
													.split("AS")[1].trim()));
						////System.out.println("column added"+tempSchema1[index].getColumnName() + " index"+index);

						} else {
							int colIndex = getColumnIndex4mSchema(pselect
									.getSelectItems().get(index).toString()
									.split("AS")[1].trim());
							// //////////////System.out.println("index2"+index);
							tempSchema1[index] = new Column(
									leftSchema[colIndex].getTable(),
									aliasToColumn
											.get(pselect.getSelectItems()
													.get(index).toString()
													.split("AS")[1].trim()));
							
						}
					} else {
						int colIndex = getColumnIndex4mSchema(pselect
								.getSelectItems().get(index).toString());
						if (pselect.getSelectItems().get(index).toString()
								.contains(".")) {
							////System.out.println("contains dot"+pselect.getSelectItems().get(index)
									//.toString().split("\\.")[0]);
							Table table = new Table();
							table.setName(pselect.getSelectItems().get(index)
									.toString().split("\\.")[0].trim());
							////System.out.println("index"+index);
							tempSchema1[index] = new Column(table, pselect
									.getSelectItems().get(index).toString()
									.split("\\.")[1].trim());
							////System.out.println("column added"+tempSchema1[index].getColumnName()+"index"+index);
						} else {
							////System.out.println("ddddd");
							tempSchema1[index] = new Column(
									leftSchema[colIndex].getTable(), pselect
											.getSelectItems().get(index)
											.toString());
						}

					}
				}
				
				
				for (int i = 0; i < pselect.getOrderByElements().size(); i++) {
					////System.out.println("orderby");
					
					oper = new OrderByOperator(oper, tempSchema1, pselect
							.getOrderByElements().get(i).toString(), pselect);
				}
			}
		}
		if (SelectItemsEvaluator.aggCheck != 1) {
			oper = new ProjectionOperator(oper, leftSchema, pselect);
				}
		if (unionCheck == 1) {
			OperatorTest.dump(oper);
			staticReset();
		} else if (FromScanner.subcheck == 1) {
			// //////////System.out.println("here");
			staticReset();
		}
	}

	public void staticReset() {
		tables = new ArrayList<String>();
		check = 0;
		scheck = 0;
		// oper.reset();
		DatumDirectory.booleanCheck = false;
		DatumDirectory.dateCheck = false;
		DatumDirectory.doubleCheck = false;
		DatumDirectory.floatCheck = false;
		DatumDirectory.intCheck = false;
		DatumDirectory.stringCheck = false;
		FromScanner.subcheck = 0;
		SelectItemsEvaluator.aggCheck = 0;
		//JoinEvaluator.count = 0;
		selExpCount = 0;
		JoinTables.flag = 0;
		list = new ArrayList();
		// ScanOperator.tableMeta=new HashMap<String, String>();
		WhereEvaluator.check = 0;

	}

	/*
	 * public Column[] updateSchema(){
	 * 
	 * ret }
	 */
	public void visit(Union unionClause) {
		unionCheck = 1;
		// //////////////////////////System.out.println("union");
		Iterator iter = unionClause.getPlainSelects().iterator();
		while (iter.hasNext()) {
			PlainSelect plainSelect = (PlainSelect) iter.next();
			visit(plainSelect);
		}
		unionCheck = 0;

	}

	public int getColumnIndex4mSchema(String colName) {
		colIndex = 0;
		if (expCheck == 0) {
			if (colName.contains(".")) {
				for (Column col : leftSchema) {
					if (colName.equals(col.toString())) {
						break;
					} else {
						colIndex++;
					}
				}
			} else {
				for (Column col : leftSchema) {

					if (colName.equals(col.getColumnName())) {
						break;
					} else {
						colIndex++;
					}
				}
			}
			return colIndex;
		}

		else {
			// //////////////System.out.println("inside else");
			if (colName.contains(".")) {
				for (Column col : tempSchema) {
					if (colName.equals(col.toString())) {
						break;
					} else {
						colIndex++;
					}
				}
			} else {
				for (Column col : tempSchema) {
					// //////////////System.out.println("column names"+col.getColumnName());
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

	boolean isAggregate(String str) {
		if (str.contains("sum") || str.contains("COUNT") || str.contains("avg")
				|| str.contains("count") || str.contains("min")
				|| str.contains("MAX")) {
			return true;
		}
		return false;
	}

	boolean isExpression(String str) {
		if (str.contains("\\+") || str.contains("-") || str.contains("\\*")
				|| str.contains("\\/")) {

			return true;
		}

		return false;

	}

	Column[] appendSchema() {
		Column[] newSchema = new Column[leftSchema.length + selExpCount];
		int i;
		for (i = 0; i < leftSchema.length; i++) {
			Table table = leftSchema[i].getTable();
			newSchema[i] = new Column(table, leftSchema[i].getColumnName());

		}

		for (int j = 0; j < selExpCount; j++) {
			Table table = new Table();
			table.setName("C9");
			newSchema[i + j] = new Column(table, selString[j]);

		}

		return newSchema;
	}

	public Expression buildQuery(String key){

	    String query = "";
	    Expression exp=null;
	    List l=map.get(key);
	    if(l!=null){
		for(int i=0;i<l.size();i++){
			if(i>0){
				query = query + " " + "and" +" ";
			}
			query = query+l.get(i);
		}				
		
		String sqlQuery = "select * from "+key+" where "+query+";";
		
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
