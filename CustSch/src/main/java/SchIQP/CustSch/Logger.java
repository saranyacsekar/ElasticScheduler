package SchIQP.CustSch;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class Logger {
	static String qryIpBasePath=System.getenv("QRY_INPUT_PATH"); 
	static String inputPath=qryIpBasePath+"/";		 
	static String fileOp=inputPath+"exeLog.txt";
	static boolean firstEntry=true;
    public static String mode="FILE";
    static boolean writeLog=false;    
    
      /* Truncates the $QRY_INPUT_PATH/exeLog.txt contents. Invoked at the beginning of the run*/
	static void emptyLog(){		
		Process proc;	
	    String command = "> "+fileOp;
		System.out.println(command);
	        try {
			proc = Runtime.getRuntime().exec(new String[] { "bash", "-c", command });
			proc.waitFor();	
			firstEntry=false;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
	}
	
	/* Writes the given string to $QRY_INPUT_PATH/exeLog.txt */
	public static void writeLog(String str) {	

		 if(firstEntry)
			 emptyLog();
	        BufferedWriter writer;
		try{
		if(writeLog){
		if(mode.equalsIgnoreCase("FILE")) {
		if(firstEntry)
			emptyLog();
		
			writer = new BufferedWriter(new FileWriter(fileOp,true));			
			writer.write(str);	
			writer.write("\n");
			writer.close();
			//System.out.println(str);
		   
		}
		else {
			System.out.println(str);
		}
		}
		else{
			if(str.startsWith("ToLog")){
				 String tmp=str.substring(6);
				 writer = new BufferedWriter(new FileWriter(fileOp,true));
	                         writer.write(str.substring(7));
	                         writer.write("\n");
	                         writer.close();
			}
		}
	}
		catch(IOException e){
		     e.printStackTrace();
		}
	}
}
