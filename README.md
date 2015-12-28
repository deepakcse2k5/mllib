package com.genpact.jersey.first;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
 
/**
 * @author Crunchify.com
 * 
 */
 
public class HttpURLConnectionExample {
	public static void main(String[] args) {
		HttpURLConnectionExample ob=new HttpURLConnectionExample();
		String file1 = null;
		ob.readCSV(file1);
	}
		
		 public String readCSV(String file){
		
		BufferedReader crunchifyBuffer = null;
		
		try {
			String crunchifyLine;
			crunchifyBuffer = new BufferedReader(new FileReader("C:\\Univariate.csv"));
			
			// How to read file in java line by line?
			while ((crunchifyLine = crunchifyBuffer.readLine()) != null) {
				System.out.println(crunchifyLine);
				
			}
			
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (crunchifyBuffer != null) crunchifyBuffer.close();
			} catch (IOException crunchifyException) {
				crunchifyException.printStackTrace();
			}
		}
		return file;
	}
		
	
	
	// Utility which converts CSV to ArrayList using Split Operation
	/*public static ArrayList<String> crunchifyCSVtoArrayList(String crunchifyCSV) {
		ArrayList<String> crunchifyResult = new ArrayList<String>();
		
		if (crunchifyCSV != null) {
			String[] splitData = crunchifyCSV.split("\\s*,\\s*");
			for (int i = 0; i < splitData.length; i++) {
				if (!(splitData[i] == null) || !(splitData[i].length() == 0)) {
					crunchifyResult.add(splitData[i].trim());
				}
			}
		}
		
		return crunchifyResult;
	}
	*/
}
