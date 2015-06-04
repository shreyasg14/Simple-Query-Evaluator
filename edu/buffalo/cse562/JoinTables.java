package edu.buffalo.cse562;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;

public class JoinTables implements Operator {

	HashMap leftTable;
	Column[] joinedSchema;
	Operator left;
	Operator right;
	Expression expOn;
	Column[] rightSchema;
	Column[] leftSchema;
	int schemaLength;
	String rightColumn;
	static int flag;
	HashMap<String, List<Object[]>> map;
	HashMap<String, Integer> indexMap;
	List<Object[]> list = null;
	Object[] datums, dat;
	FromScanner rightFromScanner;
	List<Object[]> leftTableList = new ArrayList<Object[]>();
	

	public Column[] getSchema() {
		return this.joinedSchema;
	}

	public JoinTables(Operator left, FromScanner rightFromScanner,
			Column[] leftSchema, Expression expOn) {
		// ////System.out.println("Expression to be executed:"+expOn.toString());
		leftTable = new HashMap();
		this.left = left;
		left.reset();
		
		this.right = rightFromScanner.source;
		this.expOn = expOn;
		rightSchema = rightFromScanner.getSchema();
		this.leftSchema = leftSchema;
		this.rightFromScanner = rightFromScanner;
		schemaLength = 0;
		/*for(Column c:rightSchema){
			////System.out.println("right"+c.getColumnName());
		}*/
		joinedSchema = concat(leftSchema, rightSchema);
	
		////System.out.println("length"+joinedSchema.length);
		datums = new Object[rightSchema.length];

		map = new HashMap<String, List<Object[]>>();
		indexMap = new HashMap<String, Integer>();

		
		if (expOn != null) {
			////System.out.println("expOn"+expOn.toString());
			Object[] row = left.readOneTuple();
			while (row != null) {
				buildHash(row);
				row = left.readOneTuple();
			}

		}

		else {
			
			Object[] datum = left.readOneTuple();
			while (datum != null) {
				System.out.println("populating list");
				for(int i=0;i<datum.length;i++){
					System.out.println("tuple"+ datum[i].toString());
				}
				leftTableList.add(datum);
				datum = left.readOneTuple();
			}
		}

	}

	private void buildHash(Object[] tuple) {

		//JoinEvaluator evaluator = new JoinEvaluator();
		//expOn.accept(evaluator);
		list = new ArrayList<Object[]>();
		//
		String leftColumnName=null; 
		//= evaluator.leftColumnName;
		//rightColumn = evaluator.rightColumnName;
		
		int i;
		for (i = 0; i < leftSchema.length; i++) {
			if (leftSchema[i].getColumnName().equals(leftColumnName)) {
				break;
			}
		}
		
		if (!indexMap.containsKey(tuple[i].toString())) {

			indexMap.put(tuple[i].toString(), 1);
			list.add(tuple);

			map.put(tuple[i].toString(), list);
			//////System.out.println("tuple[i]:" + tuple[i].getValue().toString());
		}

		else {
			indexMap.put(tuple[i].toString(), (Integer) ((indexMap
					.get(tuple[i].toString()).intValue()) + 1));
			list = map.get(tuple[i].toString());
			list.add(tuple);
			map.put(tuple[i].toString(), list);

		}

	}

	public Object[] readOneTuple() {
		// ////System.out.println("Log:Right Table Read One Tuple");
		if (right == null){
			//////System.out.println("Log:Null Read One Tuple");
			return null;
		}
		// ////System.out.println("Datums:"+ datums[1].getValue().toString());
		Object[] d = new Object[joinedSchema.length];
		do {
			int i;

			if (flag == 0) {
				datums = right.readOneTuple();
			}

			// ////System.out.println("Datum value tuple:"+datums[0].getValue().toString());
			if (datums == null) {
				//////System.out.println("Log:Datum NULL Table Read One Tuple");
				//reset();
				//datums = right.readOneTuple();
				return null;
			}
			if (expOn != null) {
				//////System.out.println("Log:ExpON");
				for (i = 0; i < rightSchema.length; i++) {
					if (rightSchema[i].getColumnName().equals(rightColumn)) {
						//////System.out.println("Column to be verified:"
								//+ rightColumn);
						break;
					}
				}

				if (map.containsKey(datums[i].toString())) {

					d = concat(
							(map.get(datums[i].toString()))
									.get(flag),
							datums);
					leftTableList.add(d);

					if (flag == (map.get(datums[i].toString()))
							.size() - 1) {
						// ////System.out.println("resetting flag");
						flag = 0;
					} else
						flag++;

				}

				else {
					d = null;
				}

			}

			else {
			
				
				if (leftTableList.size() > 0) {
				
					while(leftTableList.get(flag)!=null)
					{
						
						d = concat(leftTableList.get(flag), datums);
						flag++;
						if(flag == leftTableList.size()){
							flag = 0;
						}	
						return d;
					
					}
				}
				
			}

		} while (d == null);

		return d;
	}

	public void reset() {
		right.reset();
		
	}

	public Datum[] getTuple() {
		// TODO Auto-generated method stub
		return null;
	}

	public <T> T[] concat(T[] first, T[] second) {
	System.out.println("first"+first.length+"second"+second.length);
	T[] result = Arrays.copyOf(first, first.length + second.length);
	System.arraycopy(second, 0, result, first.length, second.length);
      
	for(int i=0;i<result.length;i++){
		System.out.println("joined "+result[i].toString());
	}
		return result;

	}
}
