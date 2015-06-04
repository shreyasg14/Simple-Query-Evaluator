package edu.buffalo.cse562;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public class Test {
	
	public static void main(String args[]) throws IOException{
		long startTime = System.currentTimeMillis();
		BufferedReader b=new BufferedReader(new FileReader(new File("/home/aditya/Desktop/data/lineitem_large/lineitem.dat")));
		String s="";
		
		while((s=b.readLine())!=null){
			String[] str=s.split("\\|");
			
		}
		
		long stopTime = System.currentTimeMillis();
		long elapsedTime = stopTime - startTime;
	    System.out.println(elapsedTime);
		
		
		
	}

}
