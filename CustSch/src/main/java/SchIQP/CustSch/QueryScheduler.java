package SchIQP.CustSch;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.TimeUnit;


class exe_log{
	Timestamp start_timestamp;
	Timestamp stop_timestamp;
	float start_time;
	float stop_time;
	float duration;
	String Query_id;
	int batch_no;
	int start_file_no;
	int stop_file_no;
	int clusterIndex;
	ArrayList<Float> slackTime;
	
	public exe_log(Timestamp start_timestamp, Timestamp stop_timestamp,	float start_time, float stop_time, float duration,
							String Query_id,int batch_no, int start_file_no, int stop_file_no) {
		this.start_timestamp= start_timestamp;
		this.stop_timestamp = stop_timestamp;
		this.start_time = start_time;
		this.stop_time =  stop_time;
		this.duration =  duration;
		this.Query_id =  Query_id;
		this.batch_no = batch_no;
		this.start_file_no=start_file_no;
		this.stop_file_no=stop_file_no;
		
	}
	
	public exe_log(Timestamp start_timestamp, Timestamp stop_timestamp,	float start_time, float stop_time, float duration,
			String Query_id,int batch_no) {
	this.start_timestamp= start_timestamp;
	this.stop_timestamp = stop_timestamp;
	this.start_time = start_time;
	this.stop_time =  stop_time;
	this.duration =  duration;
	this.Query_id =  Query_id;
	this.batch_no = batch_no;

	}
	public exe_log(Timestamp start_timestamp, Timestamp stop_timestamp,	float start_time, float stop_time, float duration,
			String Query_id,int batch_no, int start_file_no, int stop_file_no, int nodeIndex) {
	this.start_timestamp= start_timestamp;
	this.stop_timestamp = stop_timestamp;
	this.start_time = start_time;
	this.stop_time =  stop_time;
	this.duration =  duration;
	this.Query_id =  Query_id;
	this.batch_no = batch_no;
	this.start_file_no=start_file_no;
	this.stop_file_no=stop_file_no;
	this.clusterIndex=nodeIndex;

}
	
	public exe_log(Timestamp start_timestamp, Timestamp stop_timestamp,	float start_time, float stop_time, float duration,
			String Query_id,int batch_no, int start_file_no, int stop_file_no, int nodeIndex,ArrayList<Float> slkTime) {
	this.start_timestamp= start_timestamp;
	this.stop_timestamp = stop_timestamp;
	this.start_time = start_time;
	this.stop_time =  stop_time;
	this.duration =  duration;
	this.Query_id =  Query_id;
	this.batch_no = batch_no;
	this.start_file_no=start_file_no;
	this.stop_file_no=stop_file_no;
	this.clusterIndex=nodeIndex;
	this.slackTime=new ArrayList<Float>();
	for(int i=0;i<slkTime.size();i++)
		this.slackTime.add(slkTime.get(i));

}
}

/* Represents the node requriements at different time points along with the query batch details which gets scheduled for each time point.*/
class MultiQrySch{	
	float totalComputeTime;
	float totalTime;
	float idleTime;	
	int schIndex;
	float totalCost;
	boolean schGenerated;
	float startTime;
	int startNumNodes;
	
	ArrayList<Integer> nodeReqTimeLst=new ArrayList<Integer>();
	ArrayList<String> qryIdLst=new ArrayList<String>();
	ArrayList<Integer> reqNumNodesLst=new ArrayList<Integer>();
	ArrayList<Integer> qryBatchNoLst=new ArrayList<Integer>();	
	//ArrayList<Integer> reqIssued=new ArrayList<>();
	//ArrayList<Integer> reqIssuedTime=new ArrayList<>();
	public MultiQrySch(float totalComputeTime,float totalTime,float idleTime) {
		this.totalComputeTime=totalComputeTime;
		this.totalTime=totalTime;
		this.idleTime=idleTime;		
		schIndex=0;
		totalCost=0;
		schGenerated=false;		
	}
	public MultiQrySch() {
		schIndex=0;
		totalCost=0;
		schGenerated=false;		
	}
}

public class QueryScheduler {
	
	public static List<exe_log> exe_log_lst=new ArrayList<exe_log>();
	
	public static float cur_time=1;
	public static float aws_req_time=1;
	public static float prev_time=0;
	
	boolean runSimu=true;
	float start_time=0.01f;
	List<Query> q_list=new ArrayList<Query>();
	List<Query> sorted_q_list=new ArrayList<Query>();
	boolean exitFlg=false;
	public static Timestamp prev_timestamp;
	public static Timestamp curr_timestamp;
	
	public static String rr_sch_qryId="-1";
	public static int curNodeIndex=Cluster.defaultNodeIndex;
	public static int selNodeIndex=Cluster.defaultNodeIndex;
	public static int reqNodeIndex=Cluster.defaultNodeIndex;
	public static int curNumNodes=Cluster.numNodes.get(Cluster.defaultNodeIndex);
	public static int selNumNodes=Cluster.numNodes.get(Cluster.defaultNodeIndex);
	public static int reqNumNodes=Cluster.numNodes.get(Cluster.defaultNodeIndex);
	
	static String qryIpBasePath=System.getenv("QRY_INPUT_PATH"); 
	static String inputPath=qryIpBasePath+"/";		 
	static String logFileOp=inputPath+"clusterResizeLog.txt";
	static boolean firstEntryLog=true;
	public static MultiQrySch multiQrySchLst=new MultiQrySch();
	//static boolean minConfigMode=false;	
	boolean firstEntry=true;
	
	QueryScheduler() {
		
		prev_timestamp= new Timestamp(System.currentTimeMillis());
		curr_timestamp= new Timestamp(System.currentTimeMillis());
		//sch_mode=sch_mode;
		//Logger.writeLog("sch mode:"+sch_mode);
	}
	
	public static void update_cur_time() {
		QueryScheduler.curr_timestamp= new Timestamp(System.currentTimeMillis());
		long diff = QueryScheduler.curr_timestamp.getTime()-QueryScheduler.prev_timestamp.getTime();
		QueryScheduler.cur_time+=diff/1000.0f;
		//QueryScheduler.cur_time+=(diff%1000)*0.001;
		
	}
	
	public static void update_prev_time() {
		QueryScheduler.prev_timestamp=QueryScheduler.curr_timestamp;
		QueryScheduler.prev_time=cur_time;
	}
	
	
	
/* Logs the cluster resize requests issued in $QRY_INPUT_PATH/clusterResizeLog.txt */
	public static void logClusterResize(String qryId, int curNodes, int selNodes,int reqNodes) {
		if(firstEntryLog)
			emptyResizeLog();
		BufferedWriter writer;
		try {
			writer = new BufferedWriter(new FileWriter(logFileOp,true));		
			String tmp="Resize from "+curNodes+" to "+selNodes+" @ "+QueryScheduler.cur_time+" for "+qryId+
					"\tReqNodes==="+reqNodes+"\n" ;
			writer.write(tmp);		
			writer.close();
		   
		}catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

/* Truncates the contents of $QRY_INPUT_PATH/clusterResizeLog.txt. Invoked once in the beginning of run*/
	public static void emptyResizeLog(){		
		Process proc;	
	    String command = "> "+logFileOp;
		System.out.println(command);
	        try {
			proc = Runtime.getRuntime().exec(new String[] { "bash", "-c", command });
			proc.waitFor();	
			firstEntryLog=false;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
	}

/* Simulates LLF without Batch Size determination*/
public void sim_llf() {
	checkForNewQueries();
	ScheduleOptimiser.genOnlyLLFSchMain(cur_time,q_list);
	System.out.println("Results available in "+System.getenv("QRY_INPUT_PATH")+"/exeLog.txt");
}

/* Simulates Fixed config (with Batch Size determination)*/
public void sim_fixed_config() {
	checkForNewQueries();
	ScheduleOptimiser.genFixedConfigSchMain(cur_time, q_list);
	System.out.println("Results available in "+System.getenv("QRY_INPUT_PATH")+"/exeLog.txt");
}

/* Simulates Elastic config determination with variable number of nodes(with Batch Size determination)*/
public void sim_elastic_config() {
	checkForNewQueries();
	ScheduleOptimiser.runSimulationMain(cur_time,firstEntry,curNodeIndex,q_list,true);
	System.out.println("Results available in "+System.getenv("QRY_INPUT_PATH")+"/exeLog.txt");
}
	
/* Main module of Scheduler which performs routine tasks
1. Invokes simulation, if runSimu is set
2. Does Schedule execution by determining the currently available query batches for processing 
3. Issues node resize requests when needed*/	
public void dyn_sch_main(String instanceGrpId)
{
	
	

	while(true) {
		
		//Logger.writeLog("**************************");
		//System.out.println("CurTime::"+QueryScheduler.cur_time+" CompTime=="+multi_SchLst.totalComputeTime+" Idle Time=="+multiQrySchLst.idleTime);
		System.out.println("CurTime::"+QueryScheduler.cur_time+" runsimu="+runSimu);
		checkForNewQueries();
		//if(cur_time>1 && runSimu) {
		if(runSimu)	{
			ScheduleOptimiser.runSimulationMain(cur_time,firstEntry,curNodeIndex,q_list,false);
			QueryScheduler.update_cur_time();
			//Logger.writeLog("cur time aft query exe:"+new Timestamp(System.currentTimeMillis()));
			//Logger.writeLog("!!!!! Query:"+this.query_id+" Batch "+this.cur_batch_no+" completed ");
			//long timeDurationMsec=query_scheduler.curr_timestamp.getTime()-query_scheduler.prev_timestamp.getTime();
			//float duration=timeDurationMsec/1000.0f;	
			QueryScheduler.update_prev_time();
			//query_scheduler.cur_time+=duration;
			System.out.println("curTime after simu:"+cur_time);
			//Logger.writeLog("Max rate supported:"+Cluster.maxIpRate+"\tMin rate supported:"+Cluster.minIpRate+"\tRSL:"+Cluster.rsfactor);
		    runSimu=false;
			System.out.println("At end of simulation RSL:"+Cluster.rsfactor);
			Logger.writeLog("At end of simulation RSL:"+Cluster.rsfactor+"\tNodeIndex="+Cluster.defaultNodeIndex+" numTimes="+Cluster.ipSizeTimes);
			System.out.println(multiQrySchLst.totalTime+" "+multiQrySchLst.totalComputeTime+" "+multiQrySchLst.idleTime+" "+Cluster.rsfactor);
			for(int m=0;m<multiQrySchLst.nodeReqTimeLst.size();m++) {
				System.out.println(multiQrySchLst.nodeReqTimeLst.get(m)+" "+multiQrySchLst.qryIdLst.get(m)+" "+multiQrySchLst.reqNumNodesLst.get(m)+" "+multiQrySchLst.qryBatchNoLst.get(m));
			}	
			for(int i=0;i<q_list.size();i++) {
				//System.out.println(simuQryList.get(i).Query_id+" "+q_list.get(i).num_tuple_total);
					
				//Cluster.rsfactor=rslFact;
				//q_list.get(i).detMinBatchSizeForMultiQryRSLBased(Cluster.defaultNodeIndex,Cluster.rsfactor);
				q_list.get(i).detMinBatchSizeForMultiQryIpRateBased(Cluster.ipSizeTimes,Cluster.defaultNodeIndex);
				//this.q_list.get(i).dynCaseMinBatchSize=simuQryList.get(i).dynCaseMinBatchSize;
				if((q_list.get(i).num_tuple_total-q_list.get(i).num_tuple_processed)>q_list.get(i).dynCaseMinBatchSize)
					q_list.get(i).simuPars.curBatchSize=q_list.get(i).dynCaseMinBatchSize;
				else
					q_list.get(i).simuPars.curBatchSize=(q_list.get(i).num_tuple_total-q_list.get(i).num_tuple_processed);
				//System.out.println(simuQryList.get(i).Query_id+" "+simuQryList.get(i).num_tuple_total+" "+simuQryList.get(i).simuPars.numTuplesProcessed+" "+q_list.get(i).num_tuple_total+" "+q_list.get(i).num_tuple_pending);
				if(Cluster.numBatchPcntForParAgg==1.0f){
					q_list.get(i).simuPars.numBatchesForAgg=(int)((q_list.get(i).num_tuple_total/q_list.get(i).simuPars.curBatchSize));
				 if(q_list.get(i).num_tuple_total%q_list.get(i).simuPars.curBatchSize>0)
			                q_list.get(i).simuPars.numBatchesForAgg++;
			}
			else{
				q_list.get(i).simuPars.numBatchesForAgg=(int)((q_list.get(i).num_tuple_total/q_list.get(i).simuPars.curBatchSize)*Cluster.numBatchPcntForParAgg);
				if(q_list.get(i).simuPars.numBatchesForAgg==0) {
					q_list.get(i).simuPars.numBatchesForAgg=q_list.get(i).num_tuple_total/q_list.get(i).simuPars.curBatchSize;
					if(q_list.get(i).num_tuple_total%q_list.get(i).simuPars.curBatchSize>0)
						q_list.get(i).simuPars.numBatchesForAgg++;
				}
			}
				//System.out.println(simuQryList.get(i).num_tuple_total+" "+simuQryList.get(i).simuPars.curBatchSize+" "+Cluster.numBatchPcntForParAgg);
				//System.out.println(simuQryList.get(i).Query_id+" "+simuQryList.get(i).num_tuple_total+" "+simuQryList.get(i).simuPars.numTuplesProcessed+" "+q_list.get(i).num_tuple_total+" "+q_list.get(i).simuPars.numBatchesForAgg+" "+simuQryList.get(i).simuPars.curBatchSize+" "+Cluster.numBatchPcntForParAgg);
				if(q_list.get(i).simuPars.numBatchesForAgg==0) {
					System.out.println("simuQryList.get(i).simuPars.numBatchesForAgg became zero");
					//System.exit(-1);
				}
			}	
			
			//if(runSimu)
				//runSimu=false;
			//break;
		}	
		
		//System.out.println("MinBatches::with RSL:"+Cluster.rsfactor);
		//for(int q=0;q<q_list.size();q++)
			//System.out.println(q_list.get(q).Query_id+" "+q_list.get(q).dynCaseMinBatchSize+" "+q_list.get(q).num_tuple_total);
		//if(cur_time>3)
		//break;
		boolean posSlack=detOptimalClusterForMultiQry();
		for(int m=multiQrySchLst.schIndex;m<multiQrySchLst.nodeReqTimeLst.size();m++) {			
		      //System.out.println("Checking for node request::"+curNumNodes + ": " + reqNumNodes+" "+multiQrySchLst.reqNumNodesLst.get(m));
		      if(QueryScheduler.cur_time>=(multiQrySchLst.nodeReqTimeLst.get(m)-Cluster.clusterInitOH) && 
		    		  reqNumNodes!=multiQrySchLst.reqNumNodesLst.get(m)) {
		    	  if(reqNumNodes>multiQrySchLst.reqNumNodesLst.get(m) && QueryScheduler.cur_time<multiQrySchLst.nodeReqTimeLst.get(m))
				  break;
			  else if(reqNumNodes>multiQrySchLst.reqNumNodesLst.get(m)){
                              if(multiQrySchLst.schIndex<(multiQrySchLst.nodeReqTimeLst.size()-1)){
                                  //if(multiQrySchLst.reqNumNodesLst.get(multiQrySchLst.schIndex+1)>multiQrySchLst.reqNumNodesLst.get(multiQrySchLst.schIndex)){
                                      if((multiQrySchLst.nodeReqTimeLst.get(multiQrySchLst.schIndex+1)-multiQrySchLst.nodeReqTimeLst.get(multiQrySchLst.schIndex))<2*Cluster.clusterInitOH){
                                      break;
                                  }
                              //}
                     
                          }	  
                          }
			  boolean reqNodeResize=true;		    	 
		    	  if(reqNodeResize) {
		    	  selNumNodes=multiQrySchLst.reqNumNodesLst.get(m);
		    	  int resizeCnt=selNumNodes;
		    	  /*if(selNumNodes<=20)
		    		  resizeCnt-=1;*/
		    	  logClusterResize(multiQrySchLst.qryIdLst.get(m),curNumNodes,selNumNodes,reqNumNodes);
		    	  aws_req_time=cur_time;
		    	  Cluster.resizeCluster(instanceGrpId,resizeCnt);	
			
		    	  selNodeIndex=Cluster.getNumNodeIndex(selNumNodes);
		    	  reqNodeIndex=Cluster.getNumNodeIndex(selNumNodes);
		    	  reqNumNodes=selNumNodes;
		    	  multiQrySchLst.schIndex=m;
		    	  }		    		
		    	  
		      }
		}
		
		Logger.writeLog("CurTime"+QueryScheduler.cur_time+" "+sorted_q_list.size()+"\tslack="+posSlack);
		//Logger.writeLog("Printing Sorted Q List:::"+sorted_q_list.size());
		if(sorted_q_list.size()==0) {
			if(QueryScheduler.cur_time>Cluster.curSessionWindEnd) {
				//QueryScheduler.print_exe_log();
				writeResToFile();
				//System.out.println("MinBatches::with RSL:"+Cluster.rsfactor);
				for(int q=0;q<q_list.size();q++)
					System.out.println(q_list.get(q).query_id+" "+q_list.get(q).dynCaseMinBatchSize);
				for(int m=0;m<multiQrySchLst.nodeReqTimeLst.size();m++) {
					System.out.println(multiQrySchLst.nodeReqTimeLst.get(m)+" "+multiQrySchLst.qryIdLst.get(m)+" "+multiQrySchLst.reqNumNodesLst.get(m)+" "+multiQrySchLst.qryBatchNoLst.get(m));
				}		
				
				break;
			}
			else {
				
				/* TBM : ForSimu */
				//Logger.writeLog("Queries are not ready for scheduling: cur Timestamp:"+cur_time+" cur Time:"+curr_timestamp);
				//print_exe_log();
				Logger.writeLog("Queries are not ready for scheduling: cur Timestamp:"+cur_time+" cur Time:"+curr_timestamp);
							
				
				checkForIpRate();
				try {
				TimeUnit.SECONDS.sleep(1);				
				} catch (InterruptedException e) {				
				e.printStackTrace();
				}
				update_cur_time();
				update_prev_time();
				//QueryScheduler.cur_time+=1; 
				
				/*Logger.writeLog("MinBatchSize");
				for(int i=0;i<q_list.size();i++)
				{
					Logger.writeLog(q_list.get(i).Query_id+"\t"+q_list.get(i).dynCaseMinBatchSize+"\t"+(q_list.get(i).num_tuple_total/q_list.get(i).dynCaseMinBatchSize));
				}*/
				//continue;
			}
		}
		
		Logger.writeLog("$$$$$$$$$$$$$$$$$$$$$$$$  curNodeIndex="+curNodeIndex+"\treqNodeIndex="+reqNodeIndex+"\tselNodeIndex="+selNodeIndex);
		Logger.writeLog("$$$$$$$$$$$$$$$$$$$$$$$$  curNumNodes="+curNumNodes+"\treqNumNodes="+reqNumNodes+"\tselNumNodes="+selNumNodes);		
		//process Query
		if(curNumNodes!=reqNumNodes) {
			//check for current num nodes
			//System.out.println(curNumNodes+" "+reqNumNodes);
			int tmp=0;
			//Logger.writeLog(Cluster.clusterInitOH);
			Logger.writeLog(cur_time+"\taws_req_time=="+aws_req_time+"\t"+(aws_req_time+Cluster.clusterInitOH));
			
			
			///* TBM : ForSimu */
			/*if(cur_time>=(aws_req_time+(Cluster.clusterInitOH-60)))
				tmp=Cluster.numNodes.get(reqNodeIndex);
			else
				tmp=Cluster.numNodes.get(curNodeIndex);*/
			
			tmp=Cluster.getCurClusterSize();
			curNumNodes=tmp;
			for(int kk=0;kk<Cluster.numNodes.size();kk++) {
			if(tmp<(Cluster.numNodes.get(kk))) 
				continue;
			else
				curNodeIndex=kk;				
			}
			
			///* TBM : ForSimu */
			/*curNumNodes=getCurNumNodes();
			curNodeIndex=getNumNodeIndex(curNumNodes);*/
			
		}
		
		 if(sorted_q_list.size()==0) {
			/* boolean nilIpRate=true;
			 for(int kk=0;kk<this.q_list.size();kk++) {
				 if(this.q_list.get(kk).completed==false) {			
					 if(this.q_list.get(kk).input_rate>0.0f)
						 nilIpRate=false;
				 }
			 }
			if(nilIpRate){
				if(minConfigMode==false){		
				minConfigMode=true;
				logClusterResize("IQRY",curNumNodes,selNumNodes,reqNumNodes);
				Cluster.resizeCluster(instanceGrpId,1,true);
				}
			}
			else {
				if(minConfigMode){
				minConfigMode=false;
				logClusterResize("IQRY",curNumNodes,selNumNodes,reqNumNodes);
				Cluster.resizeCluster(instanceGrpId,Cluster.numNodes.get(Cluster.defaultNodeIndex),true);
				}
			}*/
			continue;
		 }
		
		sorted_q_list.get(0).process_dyn_query(curNodeIndex);
		
		
		if((sorted_q_list.get(0).num_tuple_total-sorted_q_list.get(0).num_tuple_processed)<=0) {
			//Logger.writeLog("Num batches executed:"+sorted_q_list.get(0).cur_batch_no);			
			for(int z=0;z<q_list.size();z++) {
			if(q_list.get(z).query_id.equals(sorted_q_list.get(0).query_id))
				q_list.get(z).completed=true;
			}
			sorted_q_list.remove(0);
		}		
		checkForIpRate();
		/*ctr++;
		if(ctr==2)
			break;*/
		
	}
}

/* Checks if addition/deletion of queries to be processed*/
public void checkForNewQueries()
{
	
	 int index;
	 String qryIpBasePath=System.getenv("QRY_INPUT_PATH"); 
	 String inputPath=qryIpBasePath+"/";
	 File fp = new File(inputPath+"query-attr.txt"); 
	 try {
	 BufferedReader br = new BufferedReader(new FileReader(fp)); 
	 String st; 
	 boolean skipFlg=false;
	 ArrayList<String> tmp_qlist = new ArrayList<String>();
	
 	 while ((st = br.readLine()) != null) {
		//  Logger.writeLog("str from file: "+st);
		  String str[]=st.split("\\s+");
		  if(str[0].equals("exit")) {
			  exitFlg=true;
			  Logger.writeLog("Exiting");
			  break;
		  }
		  skipFlg=false;
		  //Logger.writeLog(str[0]+"   "+tmp_qlist.size());
		  tmp_qlist.add(str[0]);
		  for(int i=0;i<q_list.size();i++)
		  {
			  if(q_list.get(i).query_id.equals(str[0]))
				  skipFlg=true;
		  }
		  if(!skipFlg)
		  {			  
			  Query q1=null;
			  if(str.length==7)
				  q1=new Query(str[0],Float.parseFloat(str[1]),Float.parseFloat(str[2]),Float.parseFloat(str[3]),Float.parseFloat(str[4]),
					  str[5],str[6]);
			  else if(str.length==6)
				  q1=new Query(str[0],Float.parseFloat(str[1]),Float.parseFloat(str[2]),Float.parseFloat(str[3]),Float.parseFloat(str[4]), str[5]);
			  q_list.add(q1);
			  runSimu=true;
		  }}
 	 
 	
	 
 	 //deletion of queries
 	 for(int ii =0;ii<q_list.size();ii++) {
 		 boolean found=false;
 		 for(int jj=0;jj<tmp_qlist.size();jj++) {
 			 if(q_list.get(ii).query_id.equals(tmp_qlist.get(jj))) {
 				 found=true;
 				 break;
 			 }
 		 }
 		 if(!found) {
 			 q_list.remove(ii);
 			 ii--;
 			 runSimu=true;
 		 }
 	 }
	 
    /*Logger.writeLog("Printing Q List:::"+q_list.size());
	 for(int i=0;i<q_list.size();i++)
	  {
		Logger.writeLog(q_list.get(i).Query_id);
	  }*/
	  
	  //setting max wind end time
          float maxDeadline=1.0f;
	  for(int i=0;i<q_list.size();i++){
	  if(q_list.get(i).deadline>maxDeadline){
	  	maxDeadline=q_list.get(i).wind_end_time;
	  }
	  }
	  Cluster.curSessionWindEnd=(int)maxDeadline;	  
	 }
	 catch(Exception e)
	 {
		 Logger.writeLog("Exception:"+e);
	 }	
}

/* Sorts the queries based on least laxity/slack time */
public ArrayList<Query> sortOn_slackTime(List<Query> q_list, int nodeIndex) {
	List<Query> tmp= new ArrayList<Query>();
	ArrayList<Query> opList= new ArrayList<Query>();
	for(int i=0;i<q_list.size();i++) {
		tmp.add(q_list.get(i));
		//Logger.writeLog(tmp.get(i).Query_id+"["+tmp.get(i).multiQryAttr.get(0).slackTime+"]");
	}
	
	//sorts q_list based on least slack time
	int cnt=0;
	int size=tmp.size();
	float min_slack_time=1000.0f;
	int index=0;
	while(cnt<size) {
		min_slack_time=tmp.get(0).multiQryAttr.slackTime;
		index=0;
		for(int i=0;i<tmp.size();i++) {			
			
				if(tmp.get(i).multiQryAttr.slackTime < min_slack_time) {
					index=i;					
					min_slack_time=tmp.get(i).multiQryAttr.slackTime;
			}}
		    cnt++;
		    opList.add(tmp.get(index));
			tmp.remove(index);			
		}
			
	
	/*Logger.writeLog("sorted q list on buffer time");
	for(int i=0;i<opList.size();i++) {
		Logger.writeLog(opList.get(i).Query_id+"\t");
	}*/
	return opList;
}
	
/* Writes the result of query scheduling to $QRY_INPUT_PATH/res.txt */	
public void writeResToFile() {
	String qryIpBasePath=System.getenv("QRY_INPUT_PATH"); 
	String inputPath=qryIpBasePath+"/";	 
	String fileOp=inputPath+"res.txt";
	BufferedWriter writer;
	try {
		writer = new BufferedWriter(new FileWriter(fileOp));
	
	String tmp="";
	for(int i=0; i<exe_log_lst.size();i++) {
		tmp=exe_log_lst.get(i).start_time+"\t"+exe_log_lst.get(i).stop_time+"\t";
		tmp+=exe_log_lst.get(i).Query_id+"\t"+exe_log_lst.get(i).batch_no+"\t";
		tmp+=exe_log_lst.get(i).start_timestamp+"\t"+exe_log_lst.get(i).stop_timestamp+"\t"+exe_log_lst.get(i).duration+"\t";
		tmp+=exe_log_lst.get(i).start_file_no+"\t"+exe_log_lst.get(i).stop_file_no+"\t"+exe_log_lst.get(i).clusterIndex+"\t";
		/*for(int j=0;j<exe_log_lst.get(i).slackTime.size();j++) {
			tmp+=exe_log_lst.get(i).slackTime.get(j)+"\t";
		}*/
		tmp+="\n";
		writer.write(tmp);
	}
	
    
    writer.close();
    exitFlg=true;
	}catch (IOException e) {
	// TODO Auto-generated catch block
	e.printStackTrace();
}
}


/* Carries out actual query processing /schedule execution
   1.determines queries whose batches are ready for processing by invoking check_ip_availability_multiQry() for each query 
   2. computes slack time of the queries whose batches are ready for processing  by invoking compSlackTimeNMinCompCostForMultiQry_2() for each query
   3. sorts the quereis by invoking sortOn_slackTime()*/
public boolean detOptimalClusterForMultiQry() {
	int nodeIndex=0;
	float minTotalCost=9999.0f;
	ArrayList<ArrayList <Query>> sorted_q_list4Cluster=new ArrayList<ArrayList<Query>>();
		
		//Logger.writeLog("*******  Computing for Config::"+ c+"   *******************");
		for(int i=0;i<q_list.size();i++) {	
			if(!q_list.get(i).completed) {
			//Logger.writeLog("Computing for Qry::"+ q_list.get(i).Query_id+"   *******************");
			//q_list.get(i).detCurBatchSizeForMultiQry(c);
			if(q_list.get(i).dynCaseMinBatchSize==-1) {
				//for(int n=0;n<Cluster.numNodes.size();n++)
					//q_list.get(i).detMinBatchSizeForMultiQry(Cluster.defaultNodeIndex);
					//q_list.get(i).detMinBatchSizeForMultiQry(0);
			}
				
			//Logger.writeLog("CurBatchSize for Query"+q_list.get(i).Query_id+"  is::"+q_list.get(i).cur_batch_size);
				//q_list.get(i).compSlackTimeNMinCompCostForMultiQry_2(c);			
			}
		}
		//System.exit(0);
		ArrayList <Query> innerList=new ArrayList<Query>();
		
		for(int i=0;i<q_list.size();i++) {

			//if((!q_list.get(i).completed)&&(q_list.get(i).check_ip_availability_multiQry(Cluster.defaultNodeIndex))) {			
			if((!q_list.get(i).completed)&&(q_list.get(i).check_ip_availability_multiQry())) {
			//if((!q_list.get(i).completed)&&(q_list.get(i).check_ip_availability_multiQry(nodeIndexTmp))) {
			//if(!q_list.get(i).completed) {
				//q_list.get(i).check_ip_availability_multiQry(Cluster.defaultNodeIndex);
				//q_list.get(i).compSlackTimeNMinCompCostForMultiQry_3(c);
				q_list.get(i).compSlackTimeNMinCompCostForMultiQry_2(curNodeIndex);
				innerList.add(q_list.get(i));
				Logger.writeLog("Prev & cur BRT "+q_list.get(i).prev_batch_ready_time+" "+q_list.get(i).cur_batch_ready_time+" "+(q_list.get(i).prev_batch_ready_time<q_list.get(i).cur_batch_ready_time));
				/*if(q_list.get(i).prev_batch_ready_time<q_list.get(i).cur_batch_ready_time || q_list.get(i).cur_batch_size>0) {
					//System.out.println(q_list.get(i).Query_id+" "+q_list.get(i).prev_batch_ready_time+" "+q_list.get(i).cur_batch_ready_time+" "+q_list.get(i).cur_batch_size);
					q_list.get(i).prev_batch_ready_time=q_list.get(i).cur_batch_ready_time;
					evalQueries=true;
					Logger.writeLog("evalQueries set @ "+QueryScheduler.cur_time);
					
				}*/
				
			}				
		}
		Logger.writeLog("list of queries ready for scheduling @ "+QueryScheduler.cur_time+" is "+innerList.size());
		for(int kk=0;kk<innerList.size();kk++)
			Logger.writeLog(innerList.get(kk).query_id);
		//System.exit(0);
		ArrayList <Query> sortedLst=null;
		
		
		
		sortedLst=sortOn_slackTime(innerList,curNodeIndex);
		
	    
		sorted_q_list=sortedLst;
		

	return true;
 }
	
	



/* Determines the current input rate based on averaging the tuples recevied for 3 minutes duration.
   Sets runSimu to true, denoting simulation has to be rerun, if computed input rate exceeds the max input rate supported by the current schedule*/
public void checkForIpRate() {
	Logger.writeLog("Checking ip rate @ "+QueryScheduler.cur_time);
	for(int i=0;i<this.q_list.size();i++) {
		Logger.writeLog("Checking ip rate for qry="+this.q_list.get(i).query_id+" @ "+QueryScheduler.cur_time+" with PrevTimePt="+this.q_list.get(i).prevTimePtForRateComp);
		System.out.println("Checking ip rate for qry="+this.q_list.get(i).query_id+" @ "+QueryScheduler.cur_time+" with PrevTimePt="+this.q_list.get(i).prevTimePtForRateComp+" "+this.q_list.get(i).input_rate+" "+this.q_list.get(i).max_input_rate);
		float actIpRate=0.0f;
		if(!this.q_list.get(i).completed && QueryScheduler.cur_time<=this.q_list.get(i).wind_end_time) {
			int numTuplesOverDur=0,numTuplesStartPoint=0,numTuplesStopPoint=0;
			/*if(QueryScheduler.cur_time-this.q_list.get(i).prevTimePtForRateComp>Cluster.rateEstDur) 
				numTuplesStartPoint=this.q_list.get(i).compFileNoWithIpRate(QueryScheduler.cur_time-Cluster.rateEstDur);	
			
			else
				continue;
				//numTuplesStartPoint=this.q_list.get(i).compFileNoWithIpRate(this.q_list.get(i).prevTimePtForRateComp);*/
                        
                        if(QueryScheduler.cur_time<Cluster.rateEstDur)
                            continue;
                        numTuplesStartPoint=this.q_list.get(i).compNumFilesWithIpRate(QueryScheduler.cur_time-Cluster.rateEstDur);
			
			numTuplesStopPoint=this.q_list.get(i).compNumFilesWithIpRate(QueryScheduler.cur_time);
			numTuplesOverDur=numTuplesStopPoint-numTuplesStartPoint;
			
			//float actIpRate=numTuplesOverDur/(QueryScheduler.cur_time-this.q_list.get(i).prevTimePtForRateComp+1);
			actIpRate=numTuplesOverDur/(Cluster.rateEstDur);
			Logger.writeLog("ToLog::Computed ip rate @ "+QueryScheduler.cur_time+" is "+actIpRate+" numTuples is "+numTuplesOverDur+" prevTimePt="+this.q_list.get(i).prevTimePtForRateComp);
			if(actIpRate>this.q_list.get(i).max_input_rate*1.02){
                            this.q_list.get(i).input_rate=actIpRate;                 
                            this.q_list.get(i).max_input_rate=actIpRate;
                            runSimu=true;                      
                            Logger.writeLog("ToLog::runSimu being set true for "+this.q_list.get(i).query_id+" @ "+QueryScheduler.cur_time+" and ipRate modified to "+this.q_list.get(i).input_rate+" "+this.q_list.get(i).max_input_rate);
                            System.out.println("ToLog::runSimu being set true for "+this.q_list.get(i).query_id+" @ "+QueryScheduler.cur_time+" and ipRate modified to "+this.q_list.get(i).input_rate+" "+this.q_list.get(i).max_input_rate);
                            //update num_tutple_total
                            int extraTuples=0;
                			if(cur_time<this.q_list.get(i).wind_end_time && cur_time>=this.q_list.get(i).wind_start_time) {				
                	                    extraTuples=(int)((this.q_list.get(i).wind_end_time-cur_time)*(this.q_list.get(i).input_rate));
                	                    this.q_list.get(i).num_tuple_total=(int)(this.q_list.get(i).compFileNoWithIpRate(cur_time)+extraTuples-this.q_list.get(i).compFileNoWithIpRate(this.q_list.get(i).wind_start_time-1));
                	                   
                			}
                			else if(cur_time<q_list.get(i).wind_end_time && cur_time<q_list.get(i).wind_start_time) {				
                	                    //tmpQry.num_tuple_total=(int)(tmpQry.compFileNoWithIpRate(tmpQry.wind_end_time)-tmpQry.compFileNoWithIpRate(tmpQry.wind_start_time-1));
                	                    //Logger.writeLog(tmpQry.compFileNoWithIpRate(tmpQry.wind_end_time)+" "+tmpQry.compFileNoWithIpRate(tmpQry.wind_start_time-1)+" 22 ");
                						this.q_list.get(i).num_tuple_total=(int)((q_list.get(i).wind_end_time-q_list.get(i).wind_start_time+1)*q_list.get(i).input_rate);
                	                  
                			}
                	                else{
                	                	this.q_list.get(i).num_tuple_total=(int)(this.q_list.get(i).compFileNoWithIpRate(cur_time)-this.q_list.get(i).compFileNoWithIpRate(this.q_list.get(i).wind_start_time-1));
                	                    // Logger.writeLog(tmpQry.compFileNoWithIpRate(simuCurTime)+" "+tmpQry.compFileNoWithIpRate(tmpQry.wind_start_time-1)+" 33 ");
                	                    
                	                }
                        }                	
                        this.q_list.get(i).cur_input_rate=actIpRate;                 
                        }
			
			//this.q_list.get(i).prevNumTuplesForRateComp=actNumTuples;
            this.q_list.get(i).prevTimePtForRateComp=QueryScheduler.cur_time;
                      
	}
	
}

}
