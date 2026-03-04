
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

public class TpchFileGenerator {
	
	
	public void createOrderFiles(String sourceOrders, String targetOrders, String sourceLitems,String targetLitems, int numFiles){
	
	 
	 int opFileNo=1;
	 for(int ipFileNo=1;ipFileNo<=numFiles;ipFileNo++,opFileNo++) {
	    	 BufferedWriter bufferedWriter;
	    	 String ip_file_name=sourceOrders+Integer.toString(ipFileNo);
			 String op_file_name=targetOrders+Integer.toString(opFileNo);		
	 		  		 
	 		 String line="";
			 ArrayList<String> orderKeyLst=new ArrayList<String>(); 
			  try(BufferedReader bufferedReader = new BufferedReader(new FileReader(ip_file_name))) {	
				  System.out.println(ip_file_name);
				  boolean exitFlg_litem=false;
				  bufferedWriter = new BufferedWriter(new FileWriter(op_file_name,true));
				    orderKeyLst.clear(); 
				    while(!exitFlg_litem) {
						for(int l=1;l<10000;l++){ 
				    	line = bufferedReader.readLine();
				    	//System.out.println("************************");	
				       // System.out.println(line_litem);	
				        if(line==null)
				        	exitFlg_litem=true;
				        else{
							String words[]=line.split("\\|");
							orderKeyLst.add(words[0]);
				           	bufferedWriter.append(line);	
				           	bufferedWriter.append("\n");			        		
						}
						}
						bufferedWriter.close();	  
						createLineItemFiles(sourceLitems, targetLitems, ipFileNo, opFileNo, orderKeyLst);
						opFileNo++;
				        	op_file_name=targetOrders+Integer.toString(opFileNo);
						bufferedWriter = new BufferedWriter(new FileWriter(op_file_name,true));
						orderKeyLst.clear();
					}
					bufferedWriter.close();	  
					createLineItemFiles(sourceLitems, targetLitems, ipFileNo, opFileNo, orderKeyLst);
					orderKeyLst.clear();
				
				      
	    }
	     catch (FileNotFoundException e) {
		    // Exception handling
		} catch (IOException e) {
		    // Exception handling
		}
			
	        	
	    }
	}

	public void createLineItemFiles(String source, String target, int ipFileNo, int opFileNo, ArrayList<String> orderKeyLst){
	
	    	 BufferedWriter bufferedWriter;
	    	 String ip_file_name=source+Integer.toString(ipFileNo);
			 String op_file_name=target+Integer.toString(opFileNo);		
	 		  		 
	 		 String line="";
			  
			  try(BufferedReader bufferedReader = new BufferedReader(new FileReader(ip_file_name))) {	
				  System.out.println(ip_file_name);
				  boolean exitFlg_litem=false;
				  bufferedWriter = new BufferedWriter(new FileWriter(op_file_name,true));
				
				    while(!exitFlg_litem) {					
				    	line = bufferedReader.readLine();
				    	//System.out.println("************************");	
				       // System.out.println(line_litem);	
				        if(line==null)
				        	exitFlg_litem=true;
				        else{
							String words[]=line.split("\\|");
							boolean found=false;
							for(int k=0;k<orderKeyLst.size();k++){
								if(orderKeyLst.get(k).equals(words[0])){
									found=true;
									break;
								}
							}
							if(found){
							   	bufferedWriter.append(line);
							   	bufferedWriter.append("\n");
							}				        		
						}
					}
					bufferedWriter.close();
					//System.out.println("end of lineitem file");	  
	    }
	     catch (FileNotFoundException e) {
		    // Exception handling
		} catch (IOException e) {
		    // Exception handling
		}   	
	    
	}
	
	

public static void main(String args[]) throws IOException {
		
		if(args.length==0){
			System.out.println("Invoke with <Directory of Orders Source files> <Directory of Orders Target files> <Directory of Lineitem Source files> <Directory of Lineitem Target files> <NumFiles>");
		}
		
		String sourceOrders = args[0]+"/orders.tbl.";
	    String sourceLitems = args[2]+"/lineitem.tbl.";
	    
	    String targetOrders = args[1]+"/orders.tbl.";
	    String targetLitems = args[3]+"/lineitem.tbl.";
	    
	    TpchFileGenerator obj=new TpchFileGenerator();
	    obj.createOrderFiles(sourceOrders,targetOrders,sourceLitems,targetLitems,Integer.parseInt(args[4]));  
	    
	   
}
}
