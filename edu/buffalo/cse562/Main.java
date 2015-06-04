package  edu.buffalo.cse562;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.dbi.EnvConfigObserver;

import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.Union;



public class Main {
	
	
	public static void main(String args[]){
		//System.gc();
		long startTime = System.currentTimeMillis();
		//long beforeExec = Runtime.getRuntime().totalMemory();
		//System.out.println("before exec"+beforeExec);
		
		
		
		
		
		
		
		
		
		
		
		
		
		/*Environment env = null;
		Database db = null;
		try{
        EnvironmentConfig config = new EnvironmentConfig();
        config.setAllowCreate(true);
        env = new Environment(new File("/home/aditya/Desktop/dbEnv"),
        		config);
        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        dbConfig.setSortedDuplicates(true);
        db = env.openDatabase(null, "indexDB",dbConfig);
		}
		
		catch(DatabaseException e){
			e.printStackTrace();
		}*/
		
		File dataDir = null;
		
		ArrayList<File> sqlFiles = new ArrayList<File>();
		HashMap<String, CreateTable> sqlTables = new HashMap<String, CreateTable>();
		
		
		
		for(int argsIndex = 0; argsIndex<args.length; argsIndex++){
			
			if(args[argsIndex].equals("--data")){
				dataDir = new File(args[argsIndex+1]);
				argsIndex++;
			}
			else{
				sqlFiles.add(new File(args[argsIndex]));
			}
		}
		
		

		for(File sql:sqlFiles){
			try {
				FileReader stream = new FileReader(sql);
				CCJSqlParser parser = new CCJSqlParser(stream);
				Statement stmt;
				
				while((stmt = parser.Statement()) != null){
					
					if(stmt instanceof CreateTable){
						CreateTable ct = (CreateTable)stmt;
						sqlTables.put(ct.getTable().getName(), ct);
						//System.out.println("CT:>"+stmt.toString());
					}
					else if(stmt instanceof Select){
						SelectBody select = ((Select)stmt).getSelectBody();
					//	System.out.println("Stmt:"+stmt.toString());
						PlainSelectEvaluator evaluator=new PlainSelectEvaluator(dataDir,sqlTables);
						
						if(select instanceof PlainSelect){
							//////System.out.println("hjksdfmc,");
							PlainSelect pselect = (PlainSelect)select;
							pselect.accept(evaluator);
							OperatorTest.dump(evaluator.oper);
						}
							else if(select instanceof Union){
								
								Union union=(Union)select;
								union.accept(evaluator);
							}
							
							
						
						
					    //evaluator.oper.reset();
					    
					    
						//////System.out.println("ST:>"+select);
					}
					
					
				}	
			} catch(ParseException e){
				e.printStackTrace();
			}
			catch(IOException e){
				e.printStackTrace();
			}
			
		}
		
		//////System.out.println("Hello WOrld");
		long stopTime = System.currentTimeMillis();
		long elapsedTime = stopTime - startTime;
	  //  System.out.println(elapsedTime);
		//System.out.println("number of tuples are ::::"+SelectionOperator.count);
	    long afterExec = Runtime.getRuntime().totalMemory();
		//System.out.println("After exec:" + beforeExec +"\t After exec:"+afterExec);
	}

}
