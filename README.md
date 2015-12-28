package com.genpact.sparkui;



import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Scanner;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class HttpURLConnectionExample {

	private static final String USER_AGENT = "Mozilla/5.0";

	private static final Logger LOGGER = LoggerFactory.getLogger(HttpURLConnectionExample.class);


	/*public static String initiateMetaAawb1;

	static {

		final Properties prop = new Properties();
		try {
			prop.load(HttpURLConnectionExample.class.getResourceAsStream("/configuration.properties"));

			// get the property value and print it out
			initiateMetaAawb1 = prop.getProperty("INITIATEMETAAWB1").trim();

		} catch (IOException exception) {
			LOGGER.error(exception.getMessage());
		}
	}*/


	
	public static void main(String [] args) throws Exception{
		HttpURLConnectionExample ob1=new HttpURLConnectionExample();
		System.out.println("Please Enter Module Name and DataSet");
		String moduleName;
		String dataSet;
		Scanner scanIN=new Scanner(System.in);
		moduleName=scanIN.nextLine();
		dataSet=scanIN.nextLine();

		System.out.println(ob1.computeOperation(moduleName,dataSet));


		BufferedReader bufferReader = null;

		try {
			String line;
			bufferReader = new BufferedReader(new FileReader("C:\\Univariate.csv"));

			// read file in java line by line?
			while ((line = bufferReader.readLine()) != null) {
				System.out.println(line);

			}

		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (bufferReader != null) bufferReader.close();
			} catch (IOException exception) {
				exception.printStackTrace();
			}
		}
	}

	
	/*@SuppressWarnings("unchecked")
	@POST
	@Path(value = "/stat")
	@Produces(MediaType.TEXT_PLAIN)*/
	public String computeOperation(String moduleName,String dataSet) throws Exception {
		final String url = "http://182.95.208.242:6066/v1/submissions/create";
		final URL obj = new URL(url);
		final HttpURLConnection con = (HttpURLConnection) obj.openConnection();

		final String request = 
				"{" + "\"action\":\"CreateSubmissionRequest\","+
						"\"appArgs\":[\""+moduleName+"\",\""+dataSet+"\",\"/home/ipieawb1/Desktop/output/\",\"0,1,2,3,4,5,6,7,8,9,10,11,12\",\",\"]"+","+

		         "\"appResource\" : \"/home/ipieawb1/Desktop/spark.jar\","+ "\"clientSparkVersion\":\"1.5.1\"," + "\"environmentVariables\":{"
		         + "\"SPARK_ENV_LOADED\":\"1\""
		         + "}"+"," 
		         + "\"mainClass\":\"com.genpact.ipieawb.sparkMl.Connector\","
		         +"\"sparkProperties\":{"

				+ "\"spark.jars\":\"/home/ipieawb1/Desktop/spark.jar\"," 
				+ "\"spark.driver.supervise\":\"false\","+ "\"spark.app.name\":\""+moduleName+"\","
				+ "\"spark.eventLog.enabled\":\"true\","
				+ "\"spark.submit.deployMode\":\"cluster\","
				+ "\"spark.master\":\"spark://182.95.208.242:6060\"" 
				+ "}"
				+ "}";

		System.out.println(request);
		con.setRequestMethod("POST");
		con.setDoOutput(true);
		con.addRequestProperty("Content-Type",
				"application/json;charset=UTF-8");
		con.setRequestProperty("Content-Length",
				Integer.toString(request.length()));
		con.getOutputStream().write(request.toString().getBytes("UTF-8"));
		// add request header

		LOGGER.info("\nSending 'POST' request to URL : " + url);
		//System.out.println("Response Code : " + responseCode);

		final BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(
				con.getInputStream()));
		String inputLine;
		StringBuffer response = new StringBuffer();
		inputLine=bufferedReader.readLine();
		while (inputLine!= null) {
			response.append(inputLine); 
			inputLine=bufferedReader.readLine();
		}
		bufferedReader.close();

		// print result
		System.out.println(response.toString());
		return response.toString();

	}

	public static ArrayList<String> csvtoArrayList(String csv) {
		ArrayList<String> result = new ArrayList<String>();

		if (csv != null) {
			String[] splitData = csv.split("\\s*,\\s*");
			for (int i = 0; i < splitData.length; i++) {
				if (!(splitData[i] == null) || !(splitData[i].length() == 0)) {
					result.add(splitData[i].trim());
				}
			}
		}

		return result;

	}

}
