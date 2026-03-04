package SchIQP.CustSch;
import java.util.*;
import java.util.concurrent.TimeUnit;



import java.io.*;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.nio.file.*;

class costMdl{
	int numNodes;
	float x1CoEff;
	float offset; 
	float costPerDol;
	float costReductionFact;
	ArrayList<Integer> aggNumBatches=new ArrayList<Integer>();
	ArrayList<Float> aggX1CoEff=new ArrayList<Float>();	
	ArrayList<Float> aggOffset=new ArrayList<Float>();	
	costMdl(int numNodes,float x1CoEff, float offset, float costPerDol, float cstRedFact, ArrayList<Integer> aggNumBatches, ArrayList<Float> aggCoEff, ArrayList<Float> aggOffset){
		this.numNodes=numNodes;
		this.x1CoEff=x1CoEff;	
		this.offset=offset;
		this.costPerDol=costPerDol;
		this.costReductionFact=cstRedFact;
		for(int i=0;i<aggNumBatches.size();i++)
			this.aggNumBatches.add(aggNumBatches.get(i));
		for(int i=0;i<aggCoEff.size();i++)
			this.aggX1CoEff.add(aggCoEff.get(i));
		for(int i=0;i<aggOffset.size();i++)
			this.aggOffset.add(aggOffset.get(i));
	}	
}

class QryAttr4MultiQry{
	
	float curBatchMinCompCost;
	float compCost4SlackTime;	
	float totalCostInDol;
	float slackTime;	
	float compCostForLstBatch;
	float slackTimeWrtLstBatch;
	float compCost4SJF;
	
	
	public QryAttr4MultiQry(float curBatchMinCompCost,	float compCost4SlackTime, float totalCostInDol, float slackTime) {
		this.curBatchMinCompCost=curBatchMinCompCost;
		this.compCost4SlackTime=compCost4SlackTime;
		this.totalCostInDol=totalCostInDol;
		this.slackTime=slackTime;
	}
	public QryAttr4MultiQry(float curBatchMinCompCost,	float compCost4SlackTime, float totalCostInDol, float slackTime, float compCostForLBch,
			float slkwrtLstBatch,float compCostForSJF) {
		this.curBatchMinCompCost=curBatchMinCompCost;
		this.compCost4SlackTime=compCost4SlackTime;
		this.totalCostInDol=totalCostInDol;
		this.slackTime=slackTime;
		this.compCostForLstBatch=compCostForLBch;
		this.slackTimeWrtLstBatch=slkwrtLstBatch;
		this.compCost4SJF=compCostForSJF;
	}	
}

class SimuPars{
	int numTuplesPending;
	int numBatchesProcessed;
	int numTuplesProcessed;
	int curBatchSize;
	int curBatchNo;
	float curBatchReadyTime;
	float slackTime;
	float curBatchCompTime;
	float totalCompTime;
	float aggTime;
	int curNumNodes;
	int numBatchesForAgg;
	float curBatchAggTime;	
}

public class Query {
	String query_id;
	float deadline;	
	float wind_end_time;
	float wind_start_time;
	//float slack_time;
	float input_rate;
	float max_input_rate;
	float cur_input_rate;
	int num_tuple_total;
	float total_duration;	
	int num_tuple_processed;	
	int sch_pt_index=0;
	//int num_tuple_total_ori;
	
	boolean completed;
	//String basePath="/home/saranyac/eclipse-workspace/spark/file-ip/streaming/";
	
	String basePath;
	String qryIpBasePath=System.getenv("QRY_INPUT_PATH");		//String inputPath="/home/hadoop/qrySchAWS/"; //System.getenv("INPUT_PATH");
	String inputPath=qryIpBasePath+"/";
	
	//int num_batches;
	int cur_batch_no;
	int cur_batch_size;
	int num_ip_streams;	
	int cur_batch_tuple_cnt;
	float cur_batch_ready_time;
	float prev_batch_ready_time;
	float prevTimePtForRateComp;
	
	ArrayList<String> ip_file_name = new ArrayList<String> ();	
	
	ArrayList<costMdl> costModel = new ArrayList<costMdl>();
		
	//ArrayList<schPoints> schPtswNodes = new ArrayList<schPoints>();
	
	ArrayList<String> oFiles=new ArrayList<String>();
	ArrayList<String> lFiles=new ArrayList<String>();
	ArrayList<String> yFiles=new ArrayList<String>();
	
	int singleQrySelNodeIndex;
	int singleQrySelSchPtIndex;
	
	QryAttr4MultiQry multiQryAttr;
	int dynCaseMinBatchSize;
	
	SimuPars simuPars=new SimuPars();
	
	int[][] ipRate  =  new int[2500][2];
	
	int numBatchesForAgg;
	
	public Query(Query qry) {
		this.query_id=qry.query_id;		
		this.wind_end_time=qry.wind_end_time;
		this.wind_start_time=qry.wind_start_time;
		this.input_rate=qry.input_rate;
		this.max_input_rate=qry.max_input_rate;
		this.cur_input_rate=qry.cur_input_rate;
		this.total_duration=qry.wind_end_time-qry.wind_start_time+1;		
		this.num_tuple_total=(int)(qry.total_duration*qry.input_rate);		
		this.deadline=qry.deadline;		
		this.num_tuple_processed=0;		
		this.completed=false;
		this.cur_batch_no=1;		
		for(int i=0;i<qry.ip_file_name.size();i++) {
			this.ip_file_name.add(qry.ip_file_name.get(i));
			if(qry.ip_file_name.get(i).equals("orders")||qry.ip_file_name.get(i).equals("lineitem"))
					basePath=System.getenv("TPC_INPUT_PATH")+"/file-ip/streaming/";
			else if(qry.ip_file_name.get(i).equals("events"))
				basePath=System.getenv("YAHOO_INPUT_PATH")+"/jsonevents/";
		}
		
		this.num_ip_streams=qry.ip_file_name.size();
		init_comp_cost();		
		this.singleQrySelNodeIndex=0;
		this.singleQrySelSchPtIndex=0;
		this.prev_batch_ready_time=-1;
		this.dynCaseMinBatchSize=-1;
		this.prevTimePtForRateComp=0.0f;
		//this.num_tuple_total_ori=this.num_tuple_total;
		this.numBatchesForAgg=0;
		loadIpRate();
	} 
	
	
	public Query(String query_id,  float wind_start_time, float wind_end_time, float deadline, float input_rate, String ip_file_name)
	{
		Logger.writeLog("***********************************");
		Logger.writeLog("Query:"+query_id);
		this.query_id=query_id;	
		this.wind_end_time=wind_end_time;
		this.wind_start_time=wind_start_time;
		this.input_rate=input_rate;
		this.max_input_rate=input_rate;
		this.cur_input_rate=input_rate;
		this.total_duration=this.wind_end_time-this.wind_start_time+1;		
		this.num_tuple_total=(int)(this.total_duration*this.input_rate);		
		this.deadline=deadline;
		this.num_tuple_processed=0;		
		this.completed=false;
		this.cur_batch_no=1;		
		this.ip_file_name.add(ip_file_name);		
		this.num_ip_streams=1;				
		if(ip_file_name.equals("orders")||ip_file_name.equals("lineitem"))
				basePath=System.getenv("TPC_INPUT_PATH")+"/file-ip/streaming/";
		else if(ip_file_name.equals("events"))
			basePath=System.getenv("YAHOO_INPUT_PATH")+"/jsonevents/";
			
		Logger.writeLog("total dur:"+this.total_duration+" num_tuple_total:"+this.num_tuple_total);
		Logger.writeLog("deadline:"+this.deadline);
		init_comp_cost();		
		this.singleQrySelNodeIndex=0;
		this.singleQrySelSchPtIndex=0;
		this.prev_batch_ready_time=-1;
		this.dynCaseMinBatchSize=-1;
		this.prevTimePtForRateComp=0.0f;
		//this.num_tuple_total_ori=this.num_tuple_total;
		this.numBatchesForAgg=0;
		loadIpRate();
		
	}
	
	public Query(String query_id,  float wind_start_time, float wind_end_time, float deadline, float input_rate, String ip_file_name1,String ip_file_name2)
	{
		Logger.writeLog("***********************************");
		Logger.writeLog("Query:"+query_id);
		this.query_id=query_id;		
		this.wind_end_time=wind_end_time;
		this.wind_start_time=wind_start_time;
		this.input_rate=input_rate;
		this.max_input_rate=input_rate;
		this.cur_input_rate=input_rate;
		this.total_duration=this.wind_end_time-this.wind_start_time+1;		
		this.num_tuple_total=(int)(this.total_duration*this.input_rate);		
		this.deadline=deadline;		
		this.num_tuple_processed=0;		
		this.completed=false;
		this.cur_batch_no=1;		
		this.ip_file_name.add(ip_file_name1);
		this.ip_file_name.add(ip_file_name2);
		this.num_ip_streams=2;		
		basePath=System.getenv("TPC_INPUT_PATH")+"/file-ip/streaming/";	
		Logger.writeLog("total dur:"+this.total_duration+" num_tuple_total:"+this.num_tuple_total);
		Logger.writeLog("deadline:"+this.deadline);
		init_comp_cost();		
		this.singleQrySelNodeIndex=0;
		this.singleQrySelSchPtIndex=0;
		this.prev_batch_ready_time=-1;
		this.dynCaseMinBatchSize=-1;
		this.prevTimePtForRateComp=0.0f;
		//this.num_tuple_total_ori=this.num_tuple_total;
		this.numBatchesForAgg=0;
		loadIpRate();
		
	}
	
	public void init_comp_cost()
	{
		// comp_cost.add(new ArrayList<Integer>()); 
		 //comp_cost.add(new ArrayList<cost_model>());
		 //String ipFile="/home/hadoop/qrySchClusAWS/cost_model_aws.txt";
		 String ipFile=inputPath+"/cost_model_aws.txt";
		 //Logger.writeLog("********************************"+ipFile);
		 File fp = new File(ipFile);
		 try {
		 BufferedReader br = new BufferedReader(new FileReader(fp)); 
		 String st; 		
		 boolean read_flg=false;
		 int numNodes=0;
		 float m=0.0f,c=0.0f,costPerDol=0.0f,cstRedFact=0.0f;
		 ArrayList<Integer> numBatches=new ArrayList<Integer>();
		 ArrayList<Float> aggCoEff=new ArrayList<Float>();
		 ArrayList<Float> aggOffset=new ArrayList<Float>();
		 
		 int innerIndex=0;
		 while ((st = br.readLine()) != null) {
			  //Logger.writeLog("str from file: "+st);
			  String str[]=st.split("\\s+");
			  //Logger.writeLog("str from file: "+str[1]);
			  if((!read_flg)&&(str[1].equals(query_id)))
			  {
				  //Logger.writeLog("read flg set");
				  read_flg=true;				 
				  innerIndex=1;
				  continue;
			  }
			  if(read_flg) {
				  //Logger.writeLog(str[0]);
				  if(str[0].equals("End")) {
					  //Logger.writeLog("Adding cost model::"+numNodes+"\t"+m+"\t"+c+"\t"+costPerDol+"\t"+cstRedFact);
	                  //this.costModel.add(new costMdl(numNodes,m,c,costPerDol,cstRedFact,numBatches,aggCost));
	                  //Logger.writeLog("breaking from file read");
					  break;
				  }
		          if(innerIndex==1){
		        	  numNodes=Integer.parseInt(str[0]);
       				  m=Float.parseFloat(str[1]);
       				  c=Float.parseFloat(str[2]);
       				  costPerDol=Float.parseFloat(str[3]);
       				  cstRedFact=Float.parseFloat(str[4]);
       				  innerIndex++;
		          }
		          else { 					  
					  int num_batches=Integer.parseInt(str[0]);
					  float coeff=Float.parseFloat(str[2]);
					  float offset=Float.parseFloat(str[1]);
					  numBatches.add(num_batches);
					  aggCoEff.add(coeff);
					  aggOffset.add(offset);
					  //Logger.writeLog("num_batches="+num_batches+"\tcoeff="+coeff);
					  if(innerIndex==9) {
						  //Logger.writeLog("Adding cost model::"+numNodes+"\t"+m+"\t"+c+"\t"+costPerDol+"\t"+cstRedFact);
						  //Logger.writeLog("num_batchessize="+numBatches.size()+"\taggcostsize="+aggCost.size());
						  this.costModel.add(new costMdl(numNodes,m,c,costPerDol,cstRedFact,numBatches,aggCoEff,aggOffset));
						  numBatches.clear();
						  aggCoEff.clear();
						  aggOffset.clear();
						  innerIndex=1;
					  }
					  else
						  innerIndex++;
					  
		          }
			  }
			  //Logger.writeLog("inside while last line");
		 }
		  
		 br.close();
		
		 }
		 catch(Exception e)
		 {
			 Logger.writeLog("Exception:"+e);
		 }	 
	     
	}
	
	public float compSlackTimeGeneric(float deadline, float startTime, float compDur) {
		float slackTime=deadline-startTime-compDur;
		//Logger.writeLog("slackComp:"+deadline+"\t"+startTime+"\t"+compDur+"\t"+slackTime);
		return slackTime;
	}
	
public int detMinBatchSizeForMultiQryBaseline() {
		
		int batchSize=-1;
		int numNodeIndex=Cluster.nodeIndexForBchSize;
		
		float max_batch_cost=EstDuration(numNodeIndex,this.num_tuple_total);
		float overhead_cost_tmp = (max_batch_cost*Cluster.rsfMax)/100;
		float overhead_cost=max_batch_cost+overhead_cost_tmp;	
		Logger.writeLog(numNodeIndex+" "+this.num_tuple_total+" "+Cluster.rsfMax+" "+overhead_cost);
		for(int batchNo=1;batchNo<this.num_tuple_total;batchNo++) {
			int numTuples=(this.num_tuple_total/batchNo);			
			int pendingTuples=this.num_tuple_total-(numTuples*batchNo);
			//Logger.writeLog(this.num_tuple_total+" "+batchNo);
			float durTmp=(batchNo*(EstDuration(numNodeIndex,numTuples)))+EstDuration(numNodeIndex,pendingTuples);			
			float batchDurTmp=EstDuration(numNodeIndex,numTuples);			
			if(batchDurTmp>Cluster.cmax){
				numTuples=EstTuples(numNodeIndex,Cluster.cmax);
				//Logger.writeLog("MaxbatchTime getting applied$%$$%$%$%$%$%$%$%$%$%$%$%$%$%");
				int batches=this.num_tuple_total/numTuples;
				pendingTuples=this.num_tuple_total-(numTuples*batches);
				durTmp=(batches*(EstDuration(numNodeIndex,numTuples)))+EstDuration(numNodeIndex,pendingTuples);
			}
			int numBatchesForAggCost=batchNo;
			if(pendingTuples>0)
				numBatchesForAggCost++;
			float aggDur=this.compAggCost(numNodeIndex, numBatchesForAggCost);
			float lastBatchCost=aggDur+EstDuration(numNodeIndex,numTuples);
			/*Logger.writeLog("overhead cost::"+durTmp+"\t min cost::"+overhead_cost+"\tbatchCmpCost::"+batchDurTmp+"\tnumBatches=="+batchNo+
					"\taggDur=="+aggDur+"\tnumTuples="+numTuples);
			Logger.writeLog("lastBatchCost=="+lastBatchCost+"\t(this.deadline-this.wind_end_time)"+(this.deadline-this.wind_end_time));*/
			if((durTmp+aggDur)<=overhead_cost) {
				//if(lastBatchCost <= (this.deadline-this.wind_end_time))
				batchSize=numTuples;
			}
			else
				break;
			
		}
		
		
		Logger.writeLog(this.query_id+"::default batch size==="+batchSize);
		return batchSize;
		
	}
	
	public void detMinBatchSizeForMultiQryIpRateBased(int numTimes,int numNodeIndex) {
		Logger.writeLog("\n computing min batch size for:"+this.query_id+" with "+numTimes+" num times"+" num_tuple_total="+this.num_tuple_total);
		//Logger.writeLog("len:"+comp_cost.get(9).get(0).get_cost());
		numNodeIndex=Cluster.nodeIndexForBchSize;
		int batchSize=(int)(detMinBatchSizeForMultiQryBaseline()*numTimes);
		Logger.writeLog("batchSize before cmax check:"+batchSize);
		int batches=(this.num_tuple_total/batchSize);			
		int pendingTuples=this.num_tuple_total-(batchSize*batches);
			//Logger.writeLog(this.num_tuple_total+" "+batchNo);
			float durTmp=(batches*(EstDuration(numNodeIndex,batchSize)))+EstDuration(numNodeIndex,pendingTuples);			
			float batchDurTmp=EstDuration(numNodeIndex,batchSize);			
			if(batchDurTmp>Cluster.cmax){
				batchSize=EstTuples(Cluster.nodeIndexForBchSize,Cluster.cmax);
				Logger.writeLog("MaxbatchTime getting applied$%$$%$%$%$%$%$%$%$%$%$%$%$%$%");				
			}
			if(batchSize>this.num_tuple_total)
				batchSize=this.num_tuple_total;
		this.dynCaseMinBatchSize=batchSize;
		
		Logger.writeLog(this.query_id+" with numTimes="+numTimes+"::dyn case batch size==="+this.dynCaseMinBatchSize);
		
	}
	
	public void compSlackTimeNMinCompCostForMultiQry_2(int numNodeIndex) {
		float curBatchMinCompCost=0.0f,compCost4SlackTime=0.0f,slackTime=0.0f,totalCostInDol=0.0f;
		int numBatches=(this.num_tuple_total-this.num_tuple_processed)/this.cur_batch_size;
		int pendingTuples=(this.num_tuple_total-this.num_tuple_processed)%this.cur_batch_size;
		if(pendingTuples>0)
			numBatches++;
		//Logger.writeLog("numBatches="+numBatches);
		float aggDur=this.compAggCost(numNodeIndex, (numBatches+this.cur_batch_no-1));
		float modDeadline=this.deadline-aggDur;
		Logger.writeLog(this.query_id+"-aggCost:"+aggDur+" for batches:"+(numBatches+this.cur_batch_no-1));
		curBatchMinCompCost=EstDuration(numNodeIndex,this.cur_batch_size);
		float totalCompDur=numBatches*(curBatchMinCompCost)+EstDuration(numNodeIndex,pendingTuples);
		
		
		totalCostInDol=totalCompDur*this.costModel.get(numNodeIndex).costPerDol;
		compCost4SlackTime=totalCompDur;
		slackTime=compSlackTimeGeneric(modDeadline,QueryScheduler.cur_time,totalCompDur);
		
		float lstBatchCompCost=0.0f, slkTimeForLstBatch=0.0f;
		if(pendingTuples>0)
			lstBatchCompCost=EstDuration(numNodeIndex,pendingTuples);
		else
			lstBatchCompCost=curBatchMinCompCost;
		if(QueryScheduler.cur_time<this.wind_end_time)
			slkTimeForLstBatch=compSlackTimeGeneric(modDeadline,this.wind_end_time,lstBatchCompCost);
		else
			slkTimeForLstBatch=compSlackTimeGeneric(modDeadline,QueryScheduler.cur_time,lstBatchCompCost);
		float compCostForSJF=compCost4SlackTime+aggDur;
		/*Logger.writeLog(this.query_id+"\t nodeIndex:"+numNodeIndex+"\tslackTime::"+slackTime+"\tcurBatchSize:"+this.cur_batch_size+"\tcurBatchCompCost:"+
				curBatchMinCompCost+"\tcompCostForSlack:"+compCost4SlackTime+"\tlstBchCompCost:"+lstBatchCompCost);*/
		this.multiQryAttr=new QryAttr4MultiQry(curBatchMinCompCost,compCost4SlackTime,totalCostInDol,
				slackTime,lstBatchCompCost,slkTimeForLstBatch,compCostForSJF);
	}
	
	public boolean check_ip_availability_multiQry(int numNodeIndex) {
		//if(this.query_id.equals("Q12"))
			//System.out.println(QueryScheduler.cur_time+"::"+numNodeIndex);
		boolean ipSts=false;
		String fullPath=basePath;
		String target="";
		oFiles.clear();
		lFiles.clear();	
		yFiles.clear();
		//int start_file_no=this.num_tuple_total-this.num_tuple_pending+1;
		//int start_file_no=this.num_tuple_processed+(int)this.wind_start_time;
		//int start_file_no=this.num_tuple_processed+compFileNoWithIpRate(this.wind_start_time);
		 int start_file_no=0;
                 if(this.num_tuple_processed==0)
                     start_file_no=1;
                else if(this.wind_start_time!=1.0f)
                     start_file_no=this.num_tuple_processed+compFileNoWithIpRate(this.wind_start_time);
                else
                     start_file_no=this.num_tuple_processed;
		
		//int end_file_no=ip_num_tuple(QueryScheduler.cur_time); for fixed ip rate
		int end_file_no_actual=compFileNoWithIpRate(QueryScheduler.cur_time);
		int end_file_no=0;
		//System.out.println("end_file_no_actual="+end_file_no_actual);
                Logger.writeLog(this.query_id+" ipAvailCheck @ QueryScheduler.cur_time:::"+QueryScheduler.cur_time+" "+start_file_no+" "+end_file_no_actual);
		if(QueryScheduler.cur_time>=this.wind_end_time) {			
			ipSts=true;
			//to apply cmax after window end
			float tmpCost1=EstDuration(QueryScheduler.curNodeIndex,(end_file_no_actual-start_file_no+1))
					+compAggCost(QueryScheduler.curNodeIndex, (this.cur_batch_no+1));
			if(tmpCost1>Cluster.cmax) {
				this.dynCaseMinBatchSize=EstTuples(QueryScheduler.curNodeIndex,Cluster.cmax);
				Logger.writeLog("adjusting batch size for cmax::"+this.dynCaseMinBatchSize+"\t"+(this.num_tuple_total-this.num_tuple_processed));
				if(this.dynCaseMinBatchSize>(this.num_tuple_total-this.num_tuple_processed)) {
					this.dynCaseMinBatchSize=(this.num_tuple_total-this.num_tuple_processed)-2;
				}
				end_file_no=start_file_no+this.dynCaseMinBatchSize;
			}
			else{
				//end_file_no=(int)(this.wind_end_time*this.input_rate); //this.num_tuple_total;
				//end_file_no=this.num_tuple_total;
                                //end_file_no=(int)(this.wind_start_time+compFileNoWithIpRate(this.wind_end_time)-1);
                                end_file_no=compFileNoWithIpRate(this.wind_end_time);
                                Logger.writeLog("end_file_no set as "+this.wind_start_time+" "+compFileNoWithIpRate(this.wind_end_time)+" "+end_file_no);
			}
			if(start_file_no>end_file_no)
                            start_file_no=end_file_no;
		}
		else {
			if(QueryScheduler.cur_time<this.wind_start_time) {
				ipSts=false;
			}
			else if((end_file_no_actual-start_file_no)>=(this.dynCaseMinBatchSize-1)) {
				end_file_no=start_file_no+this.dynCaseMinBatchSize-1;
                                //end_file_no=end_file_no_actual;
				Logger.writeLog("setting end_file_no for "+this.query_id+" "+start_file_no+" "+end_file_no+" "+QueryScheduler.cur_time);
				ipSts=true;
			}
			else if(end_file_no_actual==this.num_tuple_total && end_file_no_actual>start_file_no) {
				end_file_no=end_file_no_actual;
				ipSts=true;
			}
			else {
				//check if time has elasped,then process whatever is available
				//int curBatchEnd=start_file_no+this.dynCaseMinBatchSize;
				int curBatchEnd=cur_batch_no*this.dynCaseMinBatchSize;
				if(curBatchEnd>this.num_tuple_total)
					curBatchEnd=this.num_tuple_total;
				float expTime=EstTimeForFile(curBatchEnd);
				//if(QueryScheduler.cur_time>=expTime && end_file_no_actual>start_file_no) {
                                if(QueryScheduler.cur_time>=expTime && ((end_file_no_actual-start_file_no)>(this.dynCaseMinBatchSize*0.25))) {    
					end_file_no=end_file_no_actual;
					ipSts=true;
				}                                
			}
		}
		Logger.writeLog(this.query_id+"\tQueryScheduler.cur_time:::"+QueryScheduler.cur_time+"\tstart:"+start_file_no+"\tend:"+end_file_no+"\t"+this.query_id+"dynCaseMinBatchSize:"+this.dynCaseMinBatchSize);
		
		if(ipSts) {
			
			int  batchSize=0;
			for(int ip_streams=0;ip_streams<this.num_ip_streams;ip_streams++) {
				//Logger.writeLog("+++++++++++++++++++++++++++++++++++++++ ipstream:"+ip_streams+" file name:"+this.ip_file_name.get(ip_streams));
				batchSize=0;
			for(int file_no=start_file_no;file_no<=end_file_no;file_no++,batchSize++) {
					if(this.ip_file_name.get(ip_streams).equals("orders")||this.ip_file_name.get(ip_streams).equals("lineitem"))
						target=fullPath+this.ip_file_name.get(ip_streams)+"/"+this.ip_file_name.get(ip_streams)+".tbl."+Integer.toString(file_no);
					else if(this.ip_file_name.get(ip_streams).equals("events"))
						target=fullPath+"/EVENTS_FILE-"+Integer.toString(file_no)+".txt";
					//target=fullPath+this.ip_file_name.get(ip_streams)+"-new/"+this.ip_file_name.get(ip_streams)+".tbl."+Integer.toString(start_file_no+file_no);
					//Logger.writeLog("target:"+target);
					if(this.ip_file_name.get(ip_streams).startsWith("orders"))
						oFiles.add(target);
					else if (this.ip_file_name.get(ip_streams).startsWith("lineitem"))
						lFiles.add(target);				
                                         else if (this.ip_file_name.get(ip_streams).startsWith("events"))
						yFiles.add(target);
					
				}
			}
			this.cur_batch_size=batchSize;
			Logger.writeLog(this.cur_batch_size+"\t oFiles size::"+this.oFiles.size()+"\t lFiles size::"+this.lFiles.size());
			
		}
		else
			this.cur_batch_size=0;
		/*if(this.query_id.equals("Q9"))
			System.out.println(this.query_id+"\tQueryScheduler.cur_time:::"+QueryScheduler.cur_time+"\t"+this.cur_batch_size+"\tdynCaseMinBatchSize:"+
		this.dynCaseMinBatchSize.get(numNodeIndex)+":"+ipSts+":"+start_file_no+":"+end_file_no);*/
		/*if(this.cur_batch_size>0)
			ipSts=true;
		else
			ipSts=false;*/
		
		this.cur_batch_tuple_cnt=end_file_no_actual-start_file_no+1;
		if(ipSts)
			this.cur_batch_ready_time=QueryScheduler.cur_time;
		else {
			int tuplesPerSec=(int)this.input_rate/1;
			int reqTuples=this.dynCaseMinBatchSize;
			if((this.num_tuple_total-this.num_tuple_processed)<reqTuples) {
				reqTuples=(this.num_tuple_total-this.num_tuple_processed);
			}
			
			float reqTime=(reqTuples-this.cur_batch_tuple_cnt)/tuplesPerSec;
			this.cur_batch_ready_time=QueryScheduler.cur_time+reqTime;
			Logger.writeLog(this.query_id+" tuplesPerSec="+tuplesPerSec+" "+reqTime+" "+this.dynCaseMinBatchSize);
		}
		
		//System.out.println(this.query_id+" "+QueryScheduler.cur_time+" "+this.cur_batch_tuple_cnt+" "+this.dynCaseMinBatchSize+" "+this.cur_batch_ready_time+" "+start_file_no);
		//if(this.query_id.equals("Q12"))
			//System.out.println(this.query_id+"::::batch size @"+QueryScheduler.cur_time+"  "+this.cur_batch_tuple_cnt+"  "+this.cur_batch_size+"  "+this.cur_batch_ready_time);
		Logger.writeLog(this.query_id+"ipstsAndCurBatchSize="+ipSts+" "+this.cur_batch_size+" "+QueryScheduler.cur_time+" "+this.dynCaseMinBatchSize+" "+start_file_no+" "+end_file_no+" "+this.num_tuple_processed);
                return ipSts;
	}
	

	public float compAggCost(int numNodeIndex,int num_batches)
	{
		
		float cost=0.0f; 
		float coeff=0.0f;
		float offset=0.0f;
		int numBatchesFromModel=0;
		
		if(num_batches<=1)
			return 0;
		int num_nodes=Cluster.numNodes.get(numNodeIndex);
		//Logger.writeLog("inside compaggcost for numnodes="+num_nodes);
		for(int i=0;i<this.costModel.size();i++) {
			if(this.costModel.get(i).numNodes==num_nodes) {
			//	Logger.writeLog(num_nodes+" "+this.costModel.get(i).aggNumBatches.size()+" "+num_batches);
				for(int j=0;j<this.costModel.get(i).aggNumBatches.size();j++){					
					if(this.costModel.get(i).aggNumBatches.get(j)<=num_batches){
						//Logger.writeLog(i+" "+j+" "+this.costModel.get(i).aggX1CoEff.get(j)+" "+this.costModel.get(i).aggOffset.get(j)+" "+this.query_id+" "+num_nodes);
						coeff=this.costModel.get(i).aggX1CoEff.get(j);
						offset=this.costModel.get(i).aggOffset.get(j);
						numBatchesFromModel=this.costModel.get(i).aggNumBatches.get(j);
						continue;
					}
					else
						break;
				}
			}
		}
		cost=coeff*(num_batches)+offset;
		//Logger.writeLog("aggCost for "+this.query_id+" with numbatches:"+num_batches+" and nodeindex:"+numNodeIndex+"="+cost);
				
		
		
		if(cost<0) {
			System.out.println("AggCost cannot be negative:"+this.query_id+" "+numNodeIndex+" "+num_batches+" "+cost);
			System.exit(0);
		}
		/*if(num_batches>60) {
			System.out.println("AggCost cannot be > 60:"+this.query_id+" "+numNodeIndex+" "+num_batches+" "+cost);
			//System.exit(0);
		}*/
		return cost;
	}
	public float EstDuration(int nodeIndex, int num_tuples){		
		float duration=0.0f;
		//if(this.query_id.equals("Q6"))
		//System.out.println("EstDuration "+num_tuples+" : "+" with "+nodeIndex+" nodeIndex");
		//Logger.writeLog(this.costModel.get(i).x1CoEff+"\t"+this.costModel.get(i).offset);
		if(num_tuples>0)
			duration=(this.costModel.get(nodeIndex).x1CoEff*num_tuples+this.costModel.get(nodeIndex).offset);
		//Logger.writeLog("Duration for Processing "+num_tuples+" : "+duration+" with "+num_nodes+" nodes");
		//if(this.query_id.equals("Q6"))
			//System.out.println("Duration for Processing "+num_tuples+" : "+duration+" with "+nodeIndex+" nodeIndex");
		if(duration<0) {
			System.out.println("EstDuration cannot be negative:"+this.query_id+" "+nodeIndex+" "+num_tuples+" "+duration);
			System.exit(0);
		}
		return duration;
	}
	
	public int EstTuples(int nodeIndex, double duration){
		int tuples=0;
		//Logger.writeLog(this.costModel.get(i).x1CoEff+"\t"+this.costModel.get(i).offset);
		tuples=(int)((duration-this.costModel.get(nodeIndex).offset)/this.costModel.get(nodeIndex).x1CoEff);				
		if(tuples<0)
			tuples=0;
		//Logger.writeLog("Tuples processed is:"+tuples+" with "+num_nodes+" nodes");
		return tuples;
	}
	
	public int ip_num_tuple(float time_pt) {
		int num_tuple=0;
		if(time_pt>wind_end_time)
		{
			num_tuple=num_tuple_total;
		}
		else
		{
			num_tuple=(int)((time_pt-wind_start_time+1)*input_rate);
		}
		//Logger.writeLog("ip tuples @"+time_pt+" is:"+num_tuple);
		return num_tuple;
			
	}
	
	public float ip_time(int num_tuple) {
		float time=0.0f;
		if(num_tuple>=num_tuple_total) {
			time=wind_end_time;
		}
		else
		{
			time=num_tuple*input_rate+wind_start_time-1;
		}
		//Logger.writeLog("Time at which "+num_tuple+" is available:"+time);
		return time;
	}
	
	public float ip_time_modified(int num_tuple,float modiInputRate) {
		float time=0.0f;
		if(num_tuple>=num_tuple_total) {
			time=wind_end_time;
		}
		else
		{
			time=num_tuple/modiInputRate+(wind_start_time-1);			
		}
		//Logger.writeLog("Time at which "+num_tuple+" is available:"+time);
		return time;
	}
	
	public void process_dyn_query(int curNodeIndex) /* TBM : ForSimu */ //: arg not required
	{
		//System.out.println(this.query_id+" is processed @ "+QueryScheduler.cur_time);
		QueryScheduler.curr_timestamp= new Timestamp(System.currentTimeMillis());

		//Logger.writeLog("cur timestamp:"+QueryScheduler.curr_timestamp);

		QueryScheduler.update_cur_time();
		QueryScheduler.update_prev_time();
		//Logger.writeLog("$$$$$$$$  curTime:"+QueryScheduler.cur_time);
		System.out.println(this.query_id+" is going to be processed @ "+QueryScheduler.cur_time);

		Logger.writeLog("!!!!! Query:"+this.query_id+" Batch "+this.cur_batch_no+" started with BatchSize:"+this.cur_batch_size);
		//Logger.writeLog("prev timestamp:"+QueryScheduler.prev_timestamp);
		//Logger.writeLog("cur timestamp:"+QueryScheduler.curr_timestamp);
		//Logger.writeLog("cur time:"+QueryScheduler.cur_time);


		float start_time=QueryScheduler.prev_time;
		float start_time_int=QueryScheduler.cur_time;

		//Logger.writeLog("cur time aft before exe:"+new Timestamp(System.currentTimeMillis()));
		if(((this.num_tuple_processed+this.cur_batch_size)==this.num_tuple_total)&&(this.cur_batch_no>1)) 
			Logger.writeLog("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@"+"\tStarting final agg!!!!");	

		for(int x=0;x<oFiles.size();x++) {
			//System.out.println("ip:"+oFiles.get(x));
			String newFileName=oFiles.get(x);
			String parts[]=newFileName.split("\\.");
			//System.out.println(parts.length);
			int fileNum=Integer.parseInt(parts[(parts.length)-1].toString());
			if(fileNum>Cluster.curSessionWindEnd) {
				int newfileNum=fileNum%Cluster.curSessionWindEnd;
				if(newfileNum==0)
					newfileNum=Cluster.curSessionWindEnd;
				parts[(parts.length)-1]=Integer.toString(newfileNum);
				newFileName="";
				for(int y=0;y<parts.length;y++) {
					newFileName+=parts[y];
					if(y<parts.length-1)
						newFileName+=".";
				}   				
				oFiles.set(x, newFileName);
			}   		
			//System.out.println("op:"+oFiles.get(x));
		}
		for(int x=0;x<lFiles.size();x++) {
			//System.out.println("ip:"+lFiles.get(x));
			String newFileName=lFiles.get(x);
			String parts[]=newFileName.split("\\.");
			//System.out.println(parts.length);
			int fileNum=Integer.parseInt(parts[(parts.length)-1].toString());
			if(fileNum>Cluster.curSessionWindEnd) {
				int newfileNum=fileNum%Cluster.curSessionWindEnd;
				if(newfileNum==0)
					newfileNum=Cluster.curSessionWindEnd;
				parts[(parts.length)-1]=Integer.toString(newfileNum);
				newFileName="";
				for(int y=0;y<parts.length;y++) {
					newFileName+=parts[y];
					if(y<parts.length-1)
						newFileName+=".";
				}   				
				lFiles.set(x, newFileName);
			}   		
			//System.out.println("op:"+lFiles.get(x));
		}
		for(int x=0;x<yFiles.size();x++) {
			//System.out.println("ip:"+yFiles.get(x));
			String newFileName=yFiles.get(x);
			String parts[]=newFileName.split("\\-");
			String parts2[]=parts[parts.length-1].split("\\.");
			//System.out.println(parts2.length+" "+parts2[parts2.length-1]+" "+parts2[0]);
			int fileNum=Integer.parseInt(parts2[0].toString());
			if(fileNum>Cluster.curSessionWindEnd) {
				int newfileNum=fileNum%Cluster.curSessionWindEnd;
				if(newfileNum==0)
					newfileNum=Cluster.curSessionWindEnd;
				parts[(parts.length)-1]=Integer.toString(newfileNum);
				newFileName="";
				for(int y=0;y<parts.length;y++) {
					newFileName+=parts[y];
					if(y!=(parts.length-1))
						newFileName+="-";
				}   		
				newFileName+=".txt";
				//System.out.println(newFileName);		
				yFiles.set(x, newFileName);
			}   		
			//System.out.println("op:"+lFiles.get(x));
		}




		/* TBM : ForSimu */

		Timestamp start=new Timestamp(System.currentTimeMillis());	
		/*if(((this.num_tuple_processed+this.cur_batch_size)==this.num_tuple_total)&&(this.cur_batch_no>1)) 
		  TpcQueryExecutor.exe_qry(this.query_id, Integer.toString(this.cur_batch_no),2,oFiles,lFiles);
		  else
		  TpcQueryExecutor.exe_qry(this.query_id, Integer.toString(this.cur_batch_no),0,oFiles,lFiles);*/

		Map<String,ArrayList<String>> files=new HashMap<String,ArrayList<String>>();
		if(oFiles.size()>0)
			files.put("orders",oFiles);
		if(lFiles.size()>0)
			files.put("lineitem", lFiles);
		if(yFiles.size()>0)
			files.put("events",yFiles);

		float duration=0.0f;
		Timestamp end=new Timestamp(System.currentTimeMillis());;

        
  	if(((this.num_tuple_processed+this.cur_batch_size)>=this.num_tuple_total)&&(this.cur_batch_no>1)&&Cluster.numBatchPcntForParAgg==1.0f) 
  		QueryRepository.exe_qry(this.query_id, Integer.toString(this.cur_batch_no),3,files);  		
  	else if(((this.num_tuple_processed+this.cur_batch_size)>=this.num_tuple_total)&&(this.cur_batch_no>1)&&Cluster.numBatchPcntForParAgg!=1.0f) 
  		QueryRepository.exe_qry(this.query_id, Integer.toString(this.cur_batch_no),2,files);  		
  	else if(this.cur_batch_no>=this.numBatchesForAgg && (this.cur_batch_no%this.numBatchesForAgg)==0)
  		QueryRepository.exe_qry(this.query_id, Integer.toString(this.cur_batch_no),1,files);
  	else
  		QueryRepository.exe_qry(this.query_id, Integer.toString(this.cur_batch_no),0,files);
  		
  	end=new Timestamp(System.currentTimeMillis());
	QueryScheduler.update_cur_time();
	//Logger.writeLog("cur time aft query exe:"+new Timestamp(System.currentTimeMillis()));
	//Logger.writeLog("!!!!! Query:"+this.query_id+" Batch "+this.cur_batch_no+" completed ");
	long timeDurationMsec=QueryScheduler.curr_timestamp.getTime()-QueryScheduler.prev_timestamp.getTime();
	duration=timeDurationMsec/1000.0f;	
        //duration+=(timeDurationMsec%1000)*0.001;	
	QueryScheduler.update_prev_time();
	
	
	/* TBM : ForSimu */
	//QueryScheduler.cur_time+=this.multiQryAttr.get(curNodeIndex).curBatchMinCompCost; 
	/*if(query_id.equals("Q3")){
   	duration=this.multiQryAttr.curBatchMinCompCost;
	if((this.num_tuple_total-this.num_tuple_processed)==this.cur_batch_size) {
		Logger.writeLog("curNodeIndex==="+curNodeIndex);
		float aggDur=this.compAggCost(curNodeIndex,this.cur_batch_no);
		duration+=aggDur;
	}
	QueryScheduler.cur_time+=duration;

	try {
                   TimeUnit.SECONDS.sleep((int)duration);
		   //do spark submit
             } catch (InterruptedException e) {
                  // TODO Auto-generated catch block
		    e.printStackTrace();
            }

	end=new Timestamp(System.currentTimeMillis());	
	}
	*/
	/*QueryScheduler.exe_log_lst.add(new exe_log(QueryScheduler.prev_timestamp,QueryScheduler.curr_timestamp,start_time,
					QueryScheduler.cur_time,duration,this.query_id,this.cur_batch_no,(this.num_tuple_total-this.num_tuple_pending+1)
					,(this.cur_batch_size),QueryScheduler.curNumNodes));*/	
	ArrayList<Float> slkTime=new ArrayList<Float>();
	for(int ctr=0;ctr<Cluster.numNodes.size();ctr++)
		slkTime.add(this.multiQryAttr.slackTime);
	/*QueryScheduler.exe_log_lst.add(new exe_log(QueryScheduler.prev_timestamp,QueryScheduler.curr_timestamp,start_time,
			QueryScheduler.cur_time,duration,this.query_id,this.cur_batch_no,(this.num_tuple_total-this.num_tuple_pending+1)
			,(this.cur_batch_size),QueryScheduler.curNumNodes,slkTime));	*/
	
	QueryScheduler.exe_log_lst.add(new exe_log(start,end,start_time,
                        QueryScheduler.cur_time,duration,this.query_id,this.cur_batch_no,(this.num_tuple_processed)
						                        ,(this.cur_batch_size),QueryScheduler.curNumNodes,slkTime));

	this.cur_batch_no++;
	//Logger.writeLog("PendingTuples before processing:"+this.num_tuple_pending);
	this.num_tuple_processed+=this.cur_batch_size;
	Logger.writeLog("Qry:"+this.query_id+" processed @ "+QueryScheduler.cur_time+"\t PendingTuples:"+(this.num_tuple_total-this.num_tuple_processed)+
			"\t batch No:"+(this.cur_batch_no-1)+"\t curBatchCost::"+this.multiQryAttr.curBatchMinCompCost);
	
	//QueryScheduler.multiQrySchLst.idleTime-=duration;
	System.out.println("curTime after qry processing:"+QueryScheduler.cur_time+" "+start_time_int);
}





public float compTimeDiff(Timestamp startTime, Timestamp endTime) {
	java.util.Date date = new java.util.Date();
	Timestamp timestamp1 = new Timestamp(date.getTime());

	Calendar cal = Calendar.getInstance();
	cal.setTimeInMillis(startTime.getTime());

	// add a bunch of seconds to the calendar
	cal.add(Calendar.SECOND, 98765);

	// create a second time stamp
	Timestamp timestamp2 = new Timestamp(cal.getTime().getTime());

	long milliseconds = endTime.getTime() - startTime.getTime();
	int seconds = (int) milliseconds / 1000;

	int hours = seconds / 3600;
	int minutes = (seconds % 3600) / 60;
	seconds = (seconds % 3600) % 60;

	Logger.writeLog("timestamp1: " + timestamp1);
	Logger.writeLog("timestamp2: " + timestamp2);

	Logger.writeLog("Difference: ");
	Logger.writeLog(" Hours: " + hours);
	Logger.writeLog(" Minutes: " + minutes);
	Logger.writeLog(" Seconds: " + seconds);
	return 0.0f;
}

public void loadIpRate(){
	 String ipRateFileName=inputPath+"/ipRate.txt";
	 File fp = new File(ipRateFileName);
	 try {
	 BufferedReader br = new BufferedReader(new FileReader(fp)); 
	 String st; 
	 int index=0;
	 boolean read_flg=false;
	 while ((st = br.readLine()) != null) {
		  //System.out.println("str from file: "+st);
		  String str[]=st.split("\\s+");
		  this.ipRate[index][0]=Integer.parseInt(str[0]);
		  this.ipRate[index++][1]=Integer.parseInt(str[1]);
	 }
	  
	 br.close();
	
	 }
	 catch(Exception e)
	 {
		 System.out.println("Exception:"+e);
	 }	 
}

public int compFileNoWithIpRate(float time){
	int curXIndex=0;
	int fileNo=0;
	//Logger.writeLog("compFileNoWithIpRate");
        if(time<this.wind_start_time)
            return 0;
	for(int x=0; x<this.ipRate.length;x++){
		//Logger.writeLog(QueryScheduler.cur_time+"\t"+this.ipRate[x][0]);
	  if(time>=this.ipRate[x][0]){
		  curXIndex=x;
	  }
	  else
		break;
	}	
	//System.out.println("X Index is "+curXIndex);
	fileNo=((int)(time-this.ipRate[curXIndex][0]+1))*this.ipRate[curXIndex][1];
	for(int x=curXIndex-1; x>=0;x--){
		fileNo+=(this.ipRate[x+1][0]-this.ipRate[x][0])*this.ipRate[x][1];
	}
	//if(fileNo>this.num_tuple_total)
	/*if(fileNo>this.wind_end_time*this.input_rate)		
		fileNo=this.num_tuple_total;*/
	
	//if(fileNo>this.num_tuple_total)		
	//	fileNo=this.num_tuple_total;
	
	//if(fileNo>this.wind_end_time)		
	//	fileNo=(int)this.wind_end_time;
	//System.out.println(this.num_tuple_total);
	Logger.writeLog("FileNo @ "+time+" : "+fileNo+" Num_tuple_total="+this.num_tuple_total+" "+this.query_id);
	//System.out.println("FileNooo @ "+" "+time+" : "+fileNo+" Num_tuple_total="+this.num_tuple_total+" "+this.query_id);
	return fileNo;
}

public int compNumFilesWithIpRate(float time){
	int curXIndex=0;
	int fileNo=0;
        int startIndex=0;
	//Logger.writeLog("compFileNoWithIpRate");
        if(time<this.wind_start_time)
            return 0;
	for(int x=0; x<this.ipRate.length;x++){
		//Logger.writeLog(QueryScheduler.cur_time+"\t"+this.ipRate[x][0]);        
	  if(time>=this.ipRate[x][0]){
		  curXIndex=x;
	  }
	  else
		break;
	}
        for(int x=0; x<this.ipRate.length;x++){
		//Logger.writeLog(QueryScheduler.cur_time+"\t"+this.ipRate[x][0]);        
	  if(this.wind_start_time>=this.ipRate[x][0]){
		  startIndex=x;
	  }
	  else
		break;
	}
	//System.out.println("X Index is "+curXIndex);
	fileNo=((int)(time-this.ipRate[curXIndex][0]+1))*this.ipRate[curXIndex][1];
	for(int x=curXIndex-1; x>=startIndex;x--){
		fileNo+=(this.ipRate[x+1][0]-this.ipRate[x][0])*this.ipRate[x][1];
	}
	//if(fileNo>this.num_tuple_total)
	/*if(fileNo>this.wind_end_time*this.input_rate)		
		fileNo=this.num_tuple_total;*/
	
	//if(fileNo>this.num_tuple_total)		
	//	fileNo=this.num_tuple_total;
	
	//if(fileNo>this.wind_end_time)		
	//	fileNo=(int)this.wind_end_time;
	//System.out.println(this.num_tuple_total);
	//Logger.writeLog("FileNo @ "+time+" : "+fileNo+" Num_tuple_total="+this.num_tuple_total+" "+this.query_id);
	//System.out.println("FileNooo @ "+" "+time+" : "+fileNo+" Num_tuple_total="+this.num_tuple_total+" "+this.query_id);
	return fileNo;
}

public float EstTimeForFile(int fileNo) {
	Logger.writeLog("inside EstTimeForFile: ipFileNo=="+fileNo);
	float estTime=(fileNo/this.input_rate)+this.wind_start_time-1;
	Logger.writeLog("FileNo:"+fileNo+" is expected @ "+estTime);
	return estTime;
}

}
