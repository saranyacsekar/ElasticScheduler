package SchIQP.CustSch;

import java.io.IOException;
import java.util.ArrayList;
import java.io.BufferedReader;
import java.io.InputStreamReader;

import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.InstanceGroupModifyConfig;
import com.amazonaws.services.elasticmapreduce.model.ModifyInstanceGroupsRequest;

public class Cluster {
public static ArrayList<Integer> numNodes=new ArrayList<Integer>();
	
	
	public static float clusterInitOH = 8*60;
	
	public static float cmax=120;
	//public static float minBatchFactor=0.02f;
	public static float rsfactor=10.0f;
	public static float rsfMin=10.0f;
	public static float rsfMax=100.0f;
	public static int ipSizeTimes=1;
	
	/*Number of worker nodes for Configurations C1 to C5, C6 and C7 are higher configurations*/
	public static void setNumNodes() {
		Cluster.numNodes.add(2);
		Cluster.numNodes.add(4);
		Cluster.numNodes.add(10);
		Cluster.numNodes.add(14);
		Cluster.numNodes.add(20);
		Cluster.numNodes.add(24);
        Cluster.numNodes.add(30); 
		setCostFact();
	}
	public static ArrayList<Float> costFact=new ArrayList<Float>();
	
	/*Cost in dollars for each configuration. Combined Cost of EMR, EC2 based on number of instances and S3 access*/
	public static void setCostFact() {
		Cluster.costFact.add(new Float(0.0002));
		Cluster.costFact.add(new Float(0.0004));
		Cluster.costFact.add(new Float(0.001));
		Cluster.costFact.add(new Float(0.0012));
		Cluster.costFact.add(new Float(0.0016));
		Cluster.costFact.add(new Float(0.0018));
		Cluster.costFact.add(new Float(0.0024));
	}
	public static int defaultNodeIndex=0;
	
	public static int nodeIndexForBchSize=0;
			
	//public static float ipRate=5.0f;
	
	//public static float iniIpRate=1.0f;
	
	//public static float minIpRate=1.0f;
	
	public static float rateStepSize=0.1f;
	
	public static float maxRateLmt=5.0f;
	
	public static float maxIpRate=1.0f;
	
	//public static float excessNodesCostFact=0.0001f;

	//public static float minDurFact=1.5f;
	
	public static float rateEstDur=180.0f; /*Duration for input rate estimation. Averaging done over 3 minutes*/
	
	public static float numBatchPcntForParAgg=1.0f; //0.25f; /*Factor for partial aggregation. 1 denotes single final aggregation, 0.25 denotes partial aggregation at every 25% of total number of batches*/

    public static int curSessionWindEnd=4500;	/*maximum wind end. Modified based on the set of input queries */
	

    /*returns the config index for the given number of nodes */
    public static int getNumNodeIndex(int numNodes) {	
    	int nodeIndex=0;
    	for(int kk=0;kk<Cluster.numNodes.size();kk++) {
    	if(numNodes<(Cluster.numNodes.get(kk))) 
    		continue;
    	else
    		nodeIndex=kk;				
    	}
    	//System.out.println("NumNodeIndex for "+numNodes+" is "+nodeIndex);
    	return nodeIndex;
    }
    
    /*Gets the current number of nodes for the EMR cluster using AWS CLI defined in $QRY_INPUT_PATH/cluster.sh.
    CLI returns the number of nodes for each of the Primary,Core and Task instance groups. This function returns the current number of 
    worker nodes */
    
    public static int getCurClusterSize() {		

    	Logger.writeLog("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$ Expanding cluster::");
		ProcessBuilder builder = new ProcessBuilder(System.getenv("QRY_INPUT_PATH")+"/cluster.sh");
        builder.redirectErrorStream(true);
        Process proc=null;
        try {
            proc = builder.start();
        } catch (IOException ex) {
           Logger.writeLog(ex.getMessage());
        }
        InputStreamReader isr = new InputStreamReader(proc.getInputStream());
        BufferedReader reader = new BufferedReader(isr);
        String line;
        int numInstances=-1, tmp=0;
        try {
            while ((line = reader.readLine()) != null) {
                //Logger.writeLog("\n\n"+line);
			String parts[]=line.split("	");
	                //tmp=Integer.parseInt(line);
			for(int i=0;i<parts.length;i++){
				if(numInstances<Integer.parseInt(parts[i].toString()))
					numInstances=Integer.parseInt(parts[i].toString());
		}
		//Logger.writeLog("NumInstances=="+numInstances);
		/*tmp=1;
                if(numInstances<tmp)
                	numInstances=tmp;*/
            }            
        } catch (IOException ex) {
        	Logger.writeLog(ex.getMessage());
        }
        return numInstances+1;
	}


      /* Removes the intermediate results files from S3. Invoked after doing partial aggregation*/	
      public static void removeFiles(String files) {

	      Process proc;
	      String command = "aws s3 rm "+files+" --recursive  --exclude \"*\" --include \"*.csv\"";
	      Logger.writeLog("ToLog::"+command);
	      try {
	            proc = Runtime.getRuntime().exec(new String[] { "bash", "-c", command });
		    proc.waitFor();
	      }
	      catch (IOException e) {
			e.printStackTrace();
	      } catch (InterruptedException e) {
			e.printStackTrace();
	      }
      }
      
      /* Issues EMR resize request for the given Task group Id (instanceGroupId) requesting (newInstanceCount) number of nodes*/
      public static void resizeCluster(String instanceGroupId,int newInstanceCount){ 
	      
              AmazonElasticMapReduce emrClient = AmazonElasticMapReduceClientBuilder.standard()
                                     .withRegion("ap-south-1") // Replace with your AWS region
				      .build();
              InstanceGroupModifyConfig modifyConfig = new InstanceGroupModifyConfig()
                                      .withInstanceGroupId(instanceGroupId)
                                      .withInstanceCount(newInstanceCount);
              ModifyInstanceGroupsRequest request = new ModifyInstanceGroupsRequest()
                                      .withInstanceGroups(modifyConfig);
             try {
                         emrClient.modifyInstanceGroups(request);
                        
                 } catch (Exception e) {
                         System.err.println("Error resizing EMR instance group: " + e.getMessage());
                 }
      }
}
