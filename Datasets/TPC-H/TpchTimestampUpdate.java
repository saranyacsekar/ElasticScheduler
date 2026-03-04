

import java.io.IOException;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

public class TpchTimestampUpdate {
	
	
	
	public void createFiles(String source, String target, int numFields) {
		
		  int sec=0;
		  int min=0;
		  int hr=10;
		  int ctr=1;
		  
		 for(int i=1;i<=4500;i++) {
	    	 BufferedWriter bufferedWriter;
	    	 String ip_file_name=source+Integer.toString(i);
	 		 String op_file_name=target+Integer.toString(i);;
	 		 
	 		 String line="";
			 
			 
			  String secStr="";
			  String minStr="";
			  String hrStr="";
			  String customTime="";
			   
			  
			  try(BufferedReader bufferedReader = new BufferedReader(new FileReader(ip_file_name))) {	
				  System.out.println(ip_file_name);
				  boolean exitFlg_litem=false;
				  bufferedWriter = new BufferedWriter(new FileWriter(op_file_name,true));
				    while(!exitFlg_litem) {
				    	line = bufferedReader.readLine();
				    	//System.out.println("************************");	
				        //System.out.println(line);	
				        if(line==null)
				        	exitFlg_litem=true;
				        else {
				        	String words[]=line.split("\\|");
					        if(sec<10) {
					        	secStr="0"+Integer.toString(sec);
					        }
					        else {
					        	secStr=Integer.toString(sec);
					        }
					      
					        if(min<10) {
					        	minStr="0"+Integer.toString(min);
					        }
					        else {
					        	minStr=Integer.toString(min);
					        }
					        
					        if(hr<10) {
					        	hrStr="0"+Integer.toString(hr);
					        }
					        else {
					        	hrStr=Integer.toString(hr);
					        }
					        customTime=hrStr+":"+minStr+":"+secStr;
					        //System.out.println("customTime:"+customTime);
					        //words[numFields-1]=customTime;   // uncomment for adding timestamp 
					        
					       
				        	for(int j=0;j<numFields+1;j++) {
				        		
				        		if(j==(numFields)){
				        			bufferedWriter.append(customTime);
				        			bufferedWriter.append("\n");
				        		}
				        		else{
					        		bufferedWriter.append(words[j]);
				        			bufferedWriter.append("|");
			        			}
				        	}
				        	
				        	
				        	
					   }
				    }
				    bufferedWriter.close();	    
	    }
	     catch (FileNotFoundException e) {
		    // Exception handling
		} catch (IOException e) {
		    // Exception handling
		}
			 /* if(ctr==1) {
				  ctr=2;
			  }
			  else {*/
			  sec++;
	        	if(sec==60) {
	        		sec=0;
	        		min++;
	        		if(min==60) {
	        			min=0;
	        			hr++;}		    		
	    		 }
	        	ctr=1;
			 // }
	        	
	    }
	}

public static void main(String args[]) throws IOException {
		
		if(args.length==0){
			System.out.println("Invoke with <Directory of Orders Source files> <Directory of Orders Target files> <Directory of Lineitem Source files> <Directory of Lineitem Target files>");
		}
		
		String sourceOrders = args[0]+"/orders.tbl.";
	    String sourceLitems = args[2]+"/lineitem.tbl.";
	    
	    String targetOrders = args[1]+"/orders.tbl.";
	    String targetLitems = args[3]+"/lineitem.tbl.";
	    
	    TpchTimestampUpdate obj=new TpchTimestampUpdate();
	    obj.createFiles(sourceOrders,targetOrders,9);
	    obj.createFiles(sourceLitems,targetLitems,16);
	    
	   
}
}
