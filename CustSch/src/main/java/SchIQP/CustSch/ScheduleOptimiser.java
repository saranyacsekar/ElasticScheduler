package SchIQP.CustSch;

import java.util.*;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Timestamp;

class QryBchSchPointValues{

	ArrayList<Float> simuTime=new ArrayList<Float>();
	ArrayList<ArrayList<Integer>> numTuplesProcessedLst=new ArrayList<ArrayList<Integer>>();
	ArrayList<ArrayList<Integer>> numTupleTotalLst=new ArrayList<ArrayList<Integer>>();
	ArrayList<ArrayList<Integer>> numTuplesPendingLst=new ArrayList<ArrayList<Integer>>();
	ArrayList<ArrayList<Integer>> curBatchNoLst=new ArrayList<ArrayList<Integer>>();
	ArrayList<ArrayList<Integer>> numBatchesProcessedLst=new ArrayList<ArrayList<Integer>>();
	ArrayList<ArrayList<Integer>> curBatchSizeLst=new ArrayList<ArrayList<Integer>>();
	ArrayList<ArrayList<Integer>> numBatchesForAggLst=new ArrayList<ArrayList<Integer>>();
        ArrayList<ArrayList<Integer>> completionStsLst=new ArrayList<ArrayList<Integer>>();
        ArrayList<ArrayList<String>> qryIdLst=new ArrayList<ArrayList<String>>();
}

class MultiQryBchSch{	
	float totalComputeTime;
	float totalTime;
	float idleTime;		
	float totalCost;	
	float startTime;
	int startNumNodes;
        int curIndex;
        int minIndex;
        float maxTimeTBSched;
        String qryIdTBS;
        int qryBchNoTBS;
        boolean schGenerated;
	ArrayList<Float> curTime=new ArrayList<Float>();
	ArrayList<String> qryIdLst=new ArrayList<String>();
	ArrayList<Integer> reqNumNodesLst=new ArrayList<Integer>();
	ArrayList<Integer> qryBatchNoLst=new ArrayList<Integer>();
        ArrayList<Float> compStartTimeLst=new ArrayList<Float>();
        ArrayList<Float> compStopTimeLst=new ArrayList<Float>();
        ArrayList<Float> batchRdyTimeLst=new ArrayList<Float>();
	//ArrayList<Integer> reqIssued=new ArrayList<>();
	//ArrayList<Integer> reqIssuedTime=new ArrayList<>();
	public MultiQryBchSch(float totalComputeTime,float totalTime,float idleTime) {
		this.totalComputeTime=totalComputeTime;
		this.totalTime=totalTime;
		this.idleTime=idleTime;	
                this.curIndex=-1;
		//curIndex=0;
		totalCost=0;
		schGenerated=false;
	}
	public MultiQryBchSch() {
		//maxIndex=0;
		totalCost=0;
		schGenerated=false;
                this.curIndex=-1;
	}
}

public class ScheduleOptimiser {
	
	static MultiQryBchSch multiQryBchSchLst=new MultiQryBchSch();

	public static void runSimulationMain(float cur_time,boolean firstEntry,int curNodeIndex,List<Query> q_list,boolean simulation) {
		
		//if(cur_time>10)
			//return;
		//System.out.println("curTime before simu:"+cur_time);
		Logger.writeLog("ToLog::Simulation Start Time:"+new Timestamp(System.currentTimeMillis())+" cur_time="+cur_time);
		//System.out.println(this.q_list.size());
		boolean exit=false;
		float minCost=9999.0f;
		boolean schGenerated=false;
		int maxNodeReq=Cluster.numNodes.get(Cluster.numNodes.size()-1);
		//for(int c=curNodeIndex;c<AWSCluster.numNodes.size();c++) { 
		for(int c=curNodeIndex;c<5;c++) {	//AWSConfig 2 to 20 nodes
			//System.out.println("Node index=================================================="+c);
			Logger.writeLog("Node index=========="+c);
			if(firstEntry==false && c>curNodeIndex){
	                    System.out.println("No time to add additional nodes");
	                    //break;
	                }
		//for(float rslFact=AWSCluster.rsfMin;rslFact<=AWSCluster.rsfMax;rslFact+=10){
			//int numTimes=1;
			int[] numTimesLst={1,2,4,8,16,32};
		for(int x=0;x<numTimesLst.length;x++){ //batch size 1 to 32
                    //skipping smaller batch size for higher input rates and larger batch sizes for baseline input rates
                    if(firstEntry && q_list.get(0).input_rate==1.0f && x==numTimesLst.length-1)
                        continue;
                    else if(firstEntry && q_list.get(0).input_rate>1.0f && x==0)
                        continue;
	            MultiQryBchSch tmpSch=null;
			//MultiQrySch tmpSch=runSimulation(numTimesLst[x],c,0.0f,true); //TBM
	            tmpSch= runSimulation(numTimesLst[x],c,0.0f,true,cur_time,q_list); 
	              
	       if(tmpSch.schGenerated) {
				Logger.writeLog("############################# Min cost:"+tmpSch.totalCost+"\tnodeIndex="+c+"\tRSL/numTimes="+numTimesLst[x]);
				if(tmpSch.totalCost<minCost) {
					//System.out.println("############################# Setting Sch with min cost:"+tmpSch.totalCost);
					Logger.writeLog("############################# Setting Sch with min cost:"+tmpSch.totalCost+"\tnodeIndex="+c+"\tRSL/numTimes="+numTimesLst[x]);
	                                 multiQryBchSchLst.curTime.clear();
	                                 multiQryBchSchLst.qryIdLst.clear();
				     multiQryBchSchLst.reqNumNodesLst.clear();
				     multiQryBchSchLst.qryBatchNoLst.clear();
				     multiQryBchSchLst.compStartTimeLst.clear();
	                             multiQryBchSchLst.compStopTimeLst.clear();
	                             multiQryBchSchLst.batchRdyTimeLst.clear();
	                             
	                             
	        
				    
				     for(int i=0;i<tmpSch.curTime.size();i++){
	                                 multiQryBchSchLst.curTime.add(tmpSch.curTime.get(i));
	                                 multiQryBchSchLst.qryIdLst.add(tmpSch.qryIdLst.get(i));
				     multiQryBchSchLst.reqNumNodesLst.add(tmpSch.reqNumNodesLst.get(i));
				     multiQryBchSchLst.qryBatchNoLst.add(tmpSch.qryBatchNoLst.get(i));
				     multiQryBchSchLst.compStartTimeLst.add(tmpSch.compStartTimeLst.get(i));
	                             multiQryBchSchLst.compStopTimeLst.add(tmpSch.compStopTimeLst.get(i));
	                             multiQryBchSchLst.batchRdyTimeLst.add(tmpSch.batchRdyTimeLst.get(i));			         	
				         //Logger.writeLog(tmpSch.curTime.get(i)+" "+tmpSch.reqNumNodesLst.get(i));
				     }
				     //multiQrySchLst.totalTime=tmpSch.totalTime;
				     //multiQrySchLst.totalComputeTime=tmpSch.totalComputeTime;
				     //multiQrySchLst.idleTime=tmpSch.idleTime;
				     multiQryBchSchLst.totalCost=tmpSch.totalCost;
	                             
	                             
	                             QueryScheduler.multiQrySchLst.nodeReqTimeLst.clear();
	                             QueryScheduler.multiQrySchLst.qryIdLst.clear();
	                             QueryScheduler.multiQrySchLst.reqNumNodesLst.clear();
	                             QueryScheduler.multiQrySchLst.qryBatchNoLst.clear();
				     //multiQrySchLst.reqIssued.clear();
				     //multiQrySchLst.reqIssuedTime.clear();
				     for(int i=0;i<tmpSch.curTime.size();i++){                                 
				    	 QueryScheduler.multiQrySchLst.nodeReqTimeLst.add(Math.round(tmpSch.curTime.get(i)));
				    	 QueryScheduler.multiQrySchLst.qryIdLst.add(tmpSch.qryIdLst.get(i));
				    	 QueryScheduler.multiQrySchLst.reqNumNodesLst.add(tmpSch.reqNumNodesLst.get(i));
				    	 QueryScheduler.multiQrySchLst.qryBatchNoLst.add(tmpSch.qryBatchNoLst.get(i));	
				         //Logger.writeLog(tmpSch.nodeReqTimeLst.get(i)+" "+tmpSch.reqNumNodesLst.get(i));
				     }
				     //multiQrySchLst.totalTime=tmpSch.totalTime;
				     //multiQrySchLst.totalComputeTime=tmpSch.totalComputeTime;
				     //multiQrySchLst.idleTime=tmpSch.idleTime;
				     QueryScheduler.multiQrySchLst.totalCost=tmpSch.totalCost;
	                             QueryScheduler.multiQrySchLst.schIndex=0;
	                             
				     Cluster.defaultNodeIndex=c;
				     //curNodeIndex=c;
				     //AWSCluster.defaultNodeIndex=1;
				     //curNodeIndex=1;
	     			     //curNumNodes=AWSCluster.numNodes.get(curNodeIndex)-1;
				 	 //AWSCluster.rsfactor=rslFact;
				     Cluster.ipSizeTimes=numTimesLst[x];
				     //AWSCluster.ipSizeTimes=4;
				 	 minCost=tmpSch.totalCost;
				 	 schGenerated=true;
				 	 //System.out.println("While setting mincost!!!!"); 
				 	//System.out.println(multiQryBchSchLst.totalTime+" "+multiQryBchSchLst.totalComputeTime+" "+multiQrySchLst.idleTime+" "+AWSCluster.rsfactor);
					/*for(int m=0;m<multiQrySchLst.nodeReqTimeLst.size();m++) {
						System.out.println(multiQrySchLst.nodeReqTimeLst.get(m)+" "+multiQrySchLst.qryIdLst.get(m)+" "+multiQrySchLst.reqNumNodesLst.get(m)+" "+multiQrySchLst.qryBatchNoLst.get(m));
					}*/
				}
				int maxNodesFrmSch=0;
				for(int i=0;i<tmpSch.reqNumNodesLst.size();i++) {
					if(tmpSch.reqNumNodesLst.get(i)>maxNodesFrmSch) {
						maxNodesFrmSch=tmpSch.reqNumNodesLst.get(i);
					}
				}
				if(tmpSch.reqNumNodesLst.size()==0)
					maxNodesFrmSch=Cluster.numNodes.get(c);
				if(maxNodeReq>maxNodesFrmSch) {
					maxNodeReq=maxNodesFrmSch;
				}
				Logger.writeLog("maxNodeReq="+maxNodeReq+"\tmaxNodesFrmSch="+maxNodesFrmSch);
				if(Cluster.numNodes.get(c)>=maxNodesFrmSch) {
					exit=true;
					//break;
				}
				
			}
			else {
				
					//System.out.println("Sch not possible with current max nodes");
					//System.exit(-1);	
	                                   
				
			}
			//numTimes=numTimes*2;
			//Logger.writeLog("numTimes=="+numTimes);
			//break;//TBR	
		} //for rsl
		//System.out.println("Exit Flag="+exit);
		//if(exit)
			//break;
		//break; //TBR
		}
		
		//System.out.println(firstEntry);
                
                if(!simulation){
                    
		
		//Det max and min rates supported with this config
		if(firstEntry){
	        //System.out.println("Checking max and min rates supported with this config "+Cluster.rsfactor+" "+Cluster.defaultNodeIndex);
	        
		exit=false;
		float maxIpRateTmp=Cluster.rateStepSize;
		while(!exit) {
			//MultiQrySch tmpSch=runSimulation(AWSCluster.rsfactor,AWSCluster.defaultNodeIndex,maxIpRateTmp,false);
			
			MultiQryBchSch tmpSch=runSimulation(Cluster.ipSizeTimes,Cluster.defaultNodeIndex,maxIpRateTmp,false,cur_time,q_list);
			if(tmpSch.schGenerated) {
				for(int kk=0;kk<q_list.size();kk++) {
					if(q_list.get(kk).completed==false) {
						q_list.get(kk).max_input_rate=q_list.get(kk).input_rate+maxIpRateTmp;
						Logger.writeLog("To Log:: maxiprate"+q_list.get(kk).max_input_rate);
					}
				}
				//AWSCluster.maxIpRate=maxIpRateTmp;
				maxIpRateTmp+=Cluster.rateStepSize;
				for(int kk=0;kk<q_list.size();kk++) {
					if(q_list.get(kk).completed==false && q_list.get(kk).input_rate>=Cluster.maxRateLmt) 
		                break;					
				}			            
			}
			else
				exit=true;
		}
		//Logger.writeLog("Max rate supported:"+AWSCluster.maxIpRate);
	        //System.out.println("Max rate supported:"+Cluster.maxIpRate);
		
	        /*exit=false;
		float minIpRateTmp=AWSCluster.ipRate-0.1f;
		while(!exit) {		
			MultiQrySch tmpSch=runSimulation(AWSCluster.ipSizeTimes,AWSCluster.defaultNodeIndex,minIpRateTmp,false);
			if(tmpSch.schGenerated) {
				AWSCluster.minIpRate=minIpRateTmp;
				minIpRateTmp-=0.1f;
	                        if(minIpRateTmp<=AWSCluster.ipRate*5)
	                            break;
			}
			else
				exit=true;
		}*/
		//System.out.println("Min rate supported:"+AWSCluster.minIpRate);
	        firstEntry=false;
	        //Logger.writeLog("Max rate supported:"+AWSCluster.maxIpRate+"\tMin rate supported:"+AWSCluster.minIpRate+"\tRSL:"+AWSCluster.rsfactor);
	        
	        }
		for(int i=0;i<q_list.size();i++) {
			if(q_list.get(i).completed==false) {
				q_list.get(i).detMinBatchSizeForMultiQryIpRateBased(Cluster.ipSizeTimes, Cluster.defaultNodeIndex);

				 if(Cluster.numBatchPcntForParAgg==1.0f){
				        q_list.get(i).numBatchesForAgg=(int)((q_list.get(i).num_tuple_total/q_list.get(i).dynCaseMinBatchSize));
				         if(q_list.get(i).num_tuple_total%q_list.get(i).dynCaseMinBatchSize>0)
						q_list.get(i).numBatchesForAgg++;
				 }
				 else{


				q_list.get(i).numBatchesForAgg=(int)((q_list.get(i).num_tuple_total/q_list.get(i).dynCaseMinBatchSize)*Cluster.numBatchPcntForParAgg);
				if(q_list.get(i).numBatchesForAgg==0) {
					q_list.get(i).numBatchesForAgg=q_list.get(i).num_tuple_total/q_list.get(i).dynCaseMinBatchSize;
					if(q_list.get(i).num_tuple_total%q_list.get(i).dynCaseMinBatchSize>0)
						q_list.get(i).numBatchesForAgg++;
				}
				}
				//System.out.println(simuQryList.get(i).num_tuple_total+" "+simuQryList.get(i).simuPars.curBatchSize+" "+AWSCluster.numBatchPcntForParAgg);
				//System.out.println(simuQryList.get(i).query_id+" "+simuQryList.get(i).num_tuple_total+" "+simuQryList.get(i).simuPars.numTuplesProcessed+" "+q_list.get(i).num_tuple_total+" "+q_list.get(i).simuPars.numBatchesForAgg+" "+simuQryList.get(i).simuPars.curBatchSize+" "+AWSCluster.numBatchPcntForParAgg);
				/*if(q_list.get(i).numBatchesForAgg==0) {
					System.out.println("numBatchesForAgg became zero");
					System.exit(-1);
				}*/
							
			}
		}
                }
		
	        //write schedule to file
	        writeSchToFile();
		QueryScheduler.update_cur_time();
		//Logger.writeLog("cur time aft query exe:"+new Timestamp(System.currentTimeMillis()));
		//Logger.writeLog("!!!!! Query:"+this.query_id+" Batch "+this.cur_batch_no+" completed ");
		//long timeDurationMsec=query_scheduler.curr_timestamp.getTime()-query_scheduler.prev_timestamp.getTime();
		//float duration=timeDurationMsec/1000.0f;	
		QueryScheduler.update_prev_time();
		//query_scheduler.cur_time+=duration;
		//System.out.println("curTime after simu:"+cur_time);
		//Logger.writeLog("Max rate supported:"+AWSCluster.maxIpRate+"\tMin rate supported:"+AWSCluster.minIpRate+"\tRSL:"+AWSCluster.rsfactor);
	    
	    
		Logger.writeLog("ToLog::Simulation End Time:"+new Timestamp(System.currentTimeMillis())+" cur_time="+cur_time);
                //cur_time=4; //TBR
	}

	public static MultiQryBchSch runSimulation(int numTimes,int nodeIndex,float modiIpRateFact, boolean addNodes,float cur_time,List<Query> q_list) {
	    
	    		
	    int startNodeIndex=nodeIndex;
	    //System.out.println("runSimu with BatchSizeNumTimes="+numTimes+"\tnodeIndex="+nodeIndex+"\tmodiIpRateint="+modiIpRateFact+" "+cur_time);
	    Logger.writeLog("runSimu with BatchSizeNumTimes="+numTimes+"\tnodeIndex="+nodeIndex+"\tmodiIpRateint="+modiIpRateFact);
	    
	    
	  
	    //int bchSchIndex=-1;
	    
	    float simuCurTime=cur_time;
	    float simuStartTime=cur_time;
	    //float totalComputeTime=0.0f;
	    ArrayList<Query> simuQryList=new ArrayList<Query>();
	    ArrayList<Query> simuQryList2=new ArrayList<Query>();
	    ArrayList<Query> simuQryListTmp=new ArrayList<Query>();
	    
	    boolean exitFlg=false;
	        
	    /*NodeSchedule nodeSch=new NodeSchedule();
	    nodeSch.time.add(simuStartTime);
	    nodeSch.numNodeIndex.add(startNodeIndex);*/
	    
	    MultiQryBchSch bchSch = new MultiQryBchSch();
	   
	        
	     
	    simuQryList.clear();
	    simuQryList2.clear();
	    simuQryListTmp.clear();
	            
	    float minWindStartTime=9999.0f;
	    for(int i=0;i<q_list.size();i++) {			
		if(q_list.get(i).completed==false){
	            if(q_list.get(i).wind_start_time<minWindStartTime) {
			minWindStartTime=q_list.get(i).wind_start_time;
	                Logger.writeLog("simuCurTime updated to "+simuCurTime+" from "+simuCurTime);
	            }
	        }
	    }
	    if(simuCurTime<minWindStartTime)
	        simuCurTime=minWindStartTime;
	    //Logger.writeLog("simuCurTime updated to "+simuCurTime);
	    
	    bchSch.startTime=simuCurTime;
	    
	   
	    QryBchSchPointValues schPtValues =new QryBchSchPointValues();
	    ArrayList<String>  qryId=new ArrayList<String>();
	    ArrayList<Integer> numTuplesProcessed=new ArrayList<Integer>();
	    ArrayList<Integer> numTuplesTotal=new ArrayList<Integer>();
	    ArrayList<Integer> numTuplesPending=new ArrayList<Integer>();
	    ArrayList<Integer> curBatchNo=new ArrayList<Integer>();
	    ArrayList<Integer> numBatchesProcessed=new ArrayList<Integer>();
	    ArrayList<Integer> curBatchSize=new ArrayList<Integer>();
	    ArrayList<Integer> numBatchesForAgg=new ArrayList<Integer>();
	    ArrayList<Integer> completionSts=new ArrayList<Integer>();
	    
	    for(int i=0;i<q_list.size();i++) {
	        numTuplesProcessed.add(0);
	        numTuplesTotal.add(0);
	        numTuplesPending.add(0);
	        curBatchNo.add(0);
	        numBatchesProcessed.add(0);
	        curBatchSize.add(0);
	        numBatchesForAgg.add(0);
	        completionSts.add(0);
	        qryId.add("");
	    }
	        
		for(int i=0;i<q_list.size();i++) {	
	            qryId.add(i,q_list.get(i).query_id);
	            if(q_list.get(i).completed==false){	
	                Query tmpQry=new Query(q_list.get(i));
			//System.out.println(this.q_list.get(i).query_id+" "+q_list.get(i).num_tuple_total);				
			tmpQry.simuPars.numTuplesProcessed=q_list.get(i).num_tuple_processed;
	                numTuplesProcessed.set(i,tmpQry.simuPars.numTuplesProcessed);
			int extraTuples=0;
			if(simuCurTime<q_list.get(i).wind_end_time && simuCurTime>=q_list.get(i).wind_start_time) {				
	                    extraTuples=(int)((q_list.get(i).wind_end_time-simuCurTime)*(q_list.get(i).input_rate+modiIpRateFact));
	                    tmpQry.num_tuple_total=(int)(tmpQry.compFileNoWithIpRate(simuCurTime)+extraTuples-tmpQry.compFileNoWithIpRate(tmpQry.wind_start_time-1));
	                    numTuplesTotal.set(i,tmpQry.num_tuple_total);
			}
			else if(simuCurTime<q_list.get(i).wind_end_time && simuCurTime<q_list.get(i).wind_start_time) {				
	                    //tmpQry.num_tuple_total=(int)(tmpQry.compFileNoWithIpRate(tmpQry.wind_end_time)-tmpQry.compFileNoWithIpRate(tmpQry.wind_start_time-1));
	                    //Logger.writeLog(tmpQry.compFileNoWithIpRate(tmpQry.wind_end_time)+" "+tmpQry.compFileNoWithIpRate(tmpQry.wind_start_time-1)+" 22 ");
	                    tmpQry.num_tuple_total=(int)((q_list.get(i).wind_end_time-q_list.get(i).wind_start_time+1)*q_list.get(i).input_rate);
	                    numTuplesTotal.set(i,tmpQry.num_tuple_total);
			}
	                else{
	                    tmpQry.num_tuple_total=(int)(tmpQry.compFileNoWithIpRate(simuCurTime)-tmpQry.compFileNoWithIpRate(tmpQry.wind_start_time-1));
	                    // Logger.writeLog(tmpQry.compFileNoWithIpRate(simuCurTime)+" "+tmpQry.compFileNoWithIpRate(tmpQry.wind_start_time-1)+" 33 ");
	                    numTuplesTotal.set(i,tmpQry.num_tuple_total);
	                }
	                                
						
			//System.out.println(cur_time+" "+tmpQry.compFileNoWithIpRate(simuCurTime)+" "+extraTuples+" "+tmpQry.num_tuple_total+"  for checkingggg");
			
			tmpQry.simuPars.numTuplesPending=tmpQry.num_tuple_total-tmpQry.simuPars.numTuplesProcessed;
			tmpQry.simuPars.curBatchNo=q_list.get(i).cur_batch_no+1;
			tmpQry.simuPars.numBatchesProcessed=q_list.get(i).cur_batch_no;	
			//tmpQry.simuPars.numBatchesForAgg=q_list.get(i).numBatchesForAgg;
			//tmpQry.simuPars.curNumNodes=AWSCluster.numNodes.get(nodeIndex);
			//System.out.println(simuQryList.get(i).query_id+" "+simuQryList.get(i).num_tuple_pending);
	                numTuplesPending.set(i,tmpQry.simuPars.numTuplesPending);
	                curBatchNo.set(i,tmpQry.simuPars.curBatchNo);
	                numBatchesProcessed.set(i,tmpQry.simuPars.numBatchesProcessed);
	                
			simuQryList.add(tmpQry);
	                simuQryList2.add(tmpQry);  
	                simuQryListTmp.add(tmpQry);
			//System.out.println(this.q_list.get(i).query_id+" "+q_list.get(i).num_tuple_total);
			
				
			//Logger.writeLog("ToLog::Updated num_tuple_total="+this.q_list.get(i).query_id+" "+q_list.get(i).num_tuple_total+" "+tmpQry.num_tuple_total+" "+
			//tmpQry.simuPars.numTuplesProcessed+" "+q_list.get(i).wind_end_time+" "+cur_time+" "+modiIpRateFact+" "+extraTuples);
	            }
		}		
		
	        for(int i=0;i<simuQryList.size();i++) {
	            //System.out.println(simuQryList.get(i).query_id+" "+q_list.get(i).num_tuple_total);			
	            //simuQryList.get(i).detMinBatchSizeForMultiQryRSLBased(nodeIndex,rslFact);	
	            
	            //simuQryList.get(i).detMinBatchSizeForMultiQryIpRateBased(numTimes,nodeIndex);
	            simuQryList.get(i).detMinBatchSizeForMultiQryIpRateBased(numTimes,startNodeIndex);
	           
	            
	            if((simuQryList.get(i).num_tuple_total-simuQryList.get(i).num_tuple_processed)>simuQryList.get(i).dynCaseMinBatchSize)
	                simuQryList.get(i).simuPars.curBatchSize=simuQryList.get(i).dynCaseMinBatchSize;
	            else
			simuQryList.get(i).simuPars.curBatchSize=(simuQryList.get(i).num_tuple_total-simuQryList.get(i).num_tuple_processed);
		
	            curBatchSize.set(i,simuQryList.get(i).simuPars.curBatchSize);
	            
	            if(Cluster.numBatchPcntForParAgg==1.0f){
	                simuQryList.get(i).simuPars.numBatchesForAgg=(int)((simuQryList.get(i).num_tuple_total/simuQryList.get(i).simuPars.curBatchSize));
	            if(simuQryList.get(i).num_tuple_total%simuQryList.get(i).simuPars.curBatchSize>0)
	                simuQryList.get(i).simuPars.numBatchesForAgg++;
	            }
	            else{
	                simuQryList.get(i).simuPars.numBatchesForAgg=(int)((simuQryList.get(i).num_tuple_total/simuQryList.get(i).simuPars.curBatchSize)*Cluster.numBatchPcntForParAgg);
			if(simuQryList.get(i).simuPars.numBatchesForAgg==0) {
	                    simuQryList.get(i).simuPars.numBatchesForAgg=simuQryList.get(i).num_tuple_total/simuQryList.get(i).simuPars.curBatchSize;
			if(simuQryList.get(i).num_tuple_total%simuQryList.get(i).simuPars.curBatchSize>0)
	                    simuQryList.get(i).simuPars.numBatchesForAgg++;
			}
	            }
		
	            numBatchesForAgg.set(i, simuQryList.get(i).simuPars.numBatchesForAgg);
	            //System.out.println(simuQryList.get(i).num_tuple_total+" "+simuQryList.get(i).simuPars.curBatchSize+" "+AWSCluster.numBatchPcntForParAgg);
	            //System.out.println(simuQryList.get(i).query_id+" "+simuQryList.get(i).num_tuple_total+" "+simuQryList.get(i).simuPars.numTuplesProcessed+" "+q_list.get(i).num_tuple_total+" "+q_list.get(i).simuPars.numBatchesForAgg+" "+simuQryList.get(i).simuPars.curBatchSize+" "+AWSCluster.numBatchPcntForParAgg);
	            /*if(simuQryList.get(i).simuPars.numBatchesForAgg==0) {
	                System.out.println("simuQryList.get(i).simuPars.numBatchesForAgg became zero");
			//System.exit(-1);
	            }*/
	            
	            completionSts.set(i,0);
				 
		}
		
		/*schPtValues.simuTime.add(simuStartTime);
		schPtValues.numTuplesProcessedLst.add(numTuplesProcessed);
	        schPtValues.numTupleTotalLst.add(numTuplesTotal);
	        schPtValues.numTuplesPendingLst.add(numTuplesPending);
	        schPtValues.curBatchNoLst.add(curBatchNo);
	        schPtValues.numBatchesProcessedLst.add(numBatchesProcessed);
	        schPtValues.curBatchSizeLst.add(curBatchSize);
	        schPtValues.numBatchesForAggLst.add(numBatchesForAgg);
	        schPtValues.completionStsLst.add(completionSts);
	        schPtValues.qryIdLst.add(qryId);*/
	        
	    while(!exitFlg){
	        
	        //System.out.println(bchSch.curIndex+" "+schPtValues.simuTime.size());
		if(bchSch.curIndex==-1 && simuCurTime<minWindStartTime)
	            simuCurTime=minWindStartTime;
	        else  if(bchSch.curIndex!=-1)
	            simuCurTime=schPtValues.simuTime.get(bchSch.curIndex);
	        
	        //System.out.println("simuCurTime is::"+simuCurTime);
	        simuQryListTmp.clear();
	        for(int i=0;i<q_list.size();i++) {			
	            if(q_list.get(i).completed==false){	
	                Query tmpQry=new Query(q_list.get(i));
	                simuQryListTmp.add(tmpQry);
	            }
	        }
	            
	        for(int i=0;i<simuQryListTmp.size();i++){
	            if(bchSch.curIndex!=-1){	
	            if(schPtValues.completionStsLst.get(bchSch.curIndex).get(i)==1)
	                simuQryListTmp.get(i).completed=true;
	            else
	                simuQryListTmp.get(i).completed=false;
	           
	            if(simuQryListTmp.get(i).completed==false){
	            simuQryListTmp.get(i).num_tuple_processed=0;		
	            //simuQryListTmp.get(i).completed=false;
	            simuQryListTmp.get(i).cur_batch_no=1;		
	            simuQryListTmp.get(i).prev_batch_ready_time=-1;
	            simuQryListTmp.get(i).dynCaseMinBatchSize=-1;
	            simuQryListTmp.get(i).prevTimePtForRateComp=0.0f;
	            simuQryListTmp.get(i).numBatchesForAgg=0;
	            
	            simuQryListTmp.get(i).simuPars.numTuplesProcessed=schPtValues.numTuplesProcessedLst.get(bchSch.curIndex).get(i);
	            simuQryListTmp.get(i).num_tuple_total=schPtValues.numTupleTotalLst.get(bchSch.curIndex).get(i);
	            simuQryListTmp.get(i).simuPars.numTuplesPending=schPtValues.numTuplesPendingLst.get(bchSch.curIndex).get(i);
	            simuQryListTmp.get(i).simuPars.curBatchNo=schPtValues.curBatchNoLst.get(bchSch.curIndex).get(i);
	            simuQryListTmp.get(i).simuPars.numBatchesProcessed=schPtValues.numBatchesProcessedLst.get(bchSch.curIndex).get(i);
	            simuQryListTmp.get(i).simuPars.curBatchSize=schPtValues.curBatchSizeLst.get(bchSch.curIndex).get(i);
	            simuQryListTmp.get(i).simuPars.numBatchesForAgg=schPtValues.numBatchesForAggLst.get(bchSch.curIndex).get(i);          
	            
	            }
	            
	            if(i==simuQryListTmp.size()-1){
	            //remove entries from bchSch which are duplicate
	                int numEntriesTBD=bchSch.curTime.size()-bchSch.curIndex;
	                Logger.writeLog("numEntriesTBD="+numEntriesTBD+" size1="+bchSch.curTime.size()+" size2="+schPtValues.simuTime.size()+" "+bchSch.compStopTimeLst.size()+" "+bchSch.minIndex+" "+bchSch.curIndex);
	                for(int j=0;j<numEntriesTBD;j++){
	                    bchSch.qryIdLst.remove(bchSch.curTime.size()-1);
	                    bchSch.reqNumNodesLst.remove(bchSch.curTime.size()-1);
	                    bchSch.qryBatchNoLst.remove(bchSch.curTime.size()-1);
	                    bchSch.compStartTimeLst.remove(bchSch.curTime.size()-1);
	                    bchSch.compStopTimeLst.remove(bchSch.curTime.size()-1);
	                    bchSch.batchRdyTimeLst.remove(bchSch.curTime.size()-1);
	                    bchSch.curTime.remove(bchSch.curTime.size()-1);
	                }
	            	for(int j=0;j<numEntriesTBD;j++){
	            	    schPtValues.numTuplesProcessedLst.remove(schPtValues.simuTime.size()-1);	         
	        	    schPtValues.numTupleTotalLst.remove(schPtValues.simuTime.size()-1);
		            schPtValues.numTuplesPendingLst.remove(schPtValues.simuTime.size()-1);
		            schPtValues.curBatchNoLst.remove(schPtValues.simuTime.size()-1);
	        	    schPtValues.numBatchesProcessedLst.remove(schPtValues.simuTime.size()-1);
	        	    schPtValues.curBatchSizeLst.remove(schPtValues.simuTime.size()-1);
	        	    schPtValues.numBatchesForAggLst.remove(schPtValues.simuTime.size()-1);
	        	    schPtValues.completionStsLst.remove(schPtValues.simuTime.size()-1);
	        	    schPtValues.qryIdLst.remove(schPtValues.simuTime.size()-1);
	        	    schPtValues.simuTime.remove(schPtValues.simuTime.size()-1);
	            	}
	            }
	                
	            }
	            else{
	                simuQryListTmp.get(i).completed=q_list.get(i).completed;
			simuQryListTmp.get(i).num_tuple_processed=0;		
		        simuQryListTmp.get(i).cur_batch_no=1;		
	        	simuQryListTmp.get(i).prev_batch_ready_time=-1;
			simuQryListTmp.get(i).dynCaseMinBatchSize=-1;
			simuQryListTmp.get(i).prevTimePtForRateComp=0.0f;
			simuQryListTmp.get(i).numBatchesForAgg=0;
	            
	            simuQryListTmp.get(i).simuPars.numTuplesProcessed=numTuplesProcessed.get(i);
	            simuQryListTmp.get(i).num_tuple_total=numTuplesTotal.get(i);
	            simuQryListTmp.get(i).simuPars.numTuplesPending=numTuplesPending.get(i);
	            simuQryListTmp.get(i).simuPars.curBatchNo=curBatchNo.get(i);
	            simuQryListTmp.get(i).simuPars.numBatchesProcessed=numBatchesProcessed.get(i);
	            simuQryListTmp.get(i).simuPars.curBatchSize=curBatchSize.get(i);
	            simuQryListTmp.get(i).simuPars.numBatchesForAgg=numBatchesForAgg.get(i);          
	            
	            
	            }
	            
	        }
	        
	        /*Logger.writeLog("schPtValues size before deletion:"+schPtValues.simuTime.size());      
	        if(schPtValues.simuTime.size()>1){  
	            schPtValues.numTuplesProcessedLst.remove(schPtValues.simuTime.size()-1);	         
	            schPtValues.numTupleTotalLst.remove(schPtValues.simuTime.size()-1);
	            schPtValues.numTuplesPendingLst.remove(schPtValues.simuTime.size()-1);
	            schPtValues.curBatchNoLst.remove(schPtValues.simuTime.size()-1);
	            schPtValues.numBatchesProcessedLst.remove(schPtValues.simuTime.size()-1);
	            schPtValues.curBatchSizeLst.remove(schPtValues.simuTime.size()-1);
	            schPtValues.numBatchesForAggLst.remove(schPtValues.simuTime.size()-1);
	            schPtValues.completionStsLst.remove(schPtValues.simuTime.size()-1);
	            schPtValues.qryIdLst.remove(schPtValues.simuTime.size()-1);
	            schPtValues.simuTime.remove(schPtValues.simuTime.size()-1);
	        }
	        Logger.writeLog("schPtValues size after deletion:"+schPtValues.simuTime.size());*/
	        
	        Logger.writeLog("VerifyValues with curIndex::"+bchSch.curIndex+" maxTime::"+bchSch.maxTimeTBSched+" nodeIndex::"+nodeIndex+" numqueries "+simuQryListTmp.size()+" "+schPtValues.simuTime.size()+" "+bchSch.curTime.size());
	        //if(bchSch.curIndex!=-1)
	            //Logger.writeLog("VerifyValues with curIndex::"+bchSch.curIndex+" maxTime::"+bchSch.maxTimeSched+" nodeIndex::"+nodeIndex+" numqueries "+simuQryListTmp.size()+" "+schPtValues.simuTime.size()+" "+bchSch.qryIdLst.get(bchSch.curIndex));
	        //System.out.println("VerifyValues with curIndex::"+bchSch.curIndex+" maxTime::"+bchSch.maxTimeSched+" nodeIndex::"+nodeIndex+" numqueries "+simuQryListTmp.size()+" "+schPtValues.simuTime.size());
	        
	        Logger.writeLog("Printing BchSchlist before VerifyConfig");
	        if(bchSch.curTime.size()>5){
	        for(int i=bchSch.curTime.size()-5;i<bchSch.curTime.size();i++){
	            Logger.writeLog(i+" "+bchSch.qryIdLst.get(i)+" "+bchSch.compStartTimeLst.get(i)+" "+bchSch.compStopTimeLst.get(i)+" "+bchSch.reqNumNodesLst.get(i));
	            //System.out.println(i+" "+bchSch.qryIdLst.get(i)+" "+bchSch.compStartTimeLst.get(i)+" "+bchSch.compStopTimeLst.get(i)+" "+bchSch.reqNumNodesLst.get(i));
	        } 
	        Logger.writeLog("Printing SchPtValues before VerifyConfig");
	        for(int kk=schPtValues.simuTime.size()-5;kk<schPtValues.simuTime.size();kk++)
	            Logger.writeLog(schPtValues.simuTime.get(kk)+" "+schPtValues.qryIdLst.get(kk).get(0));                    
	        }               
               
		verifyConfig(simuQryListTmp,simuCurTime,bchSch,schPtValues,nodeIndex);
	        //System.out.println("Result of Verify Config="+bchSch.schGenerated+" size of SchPtValues="+schPtValues.simuTime.size());
	        

	        //System.out.println("************************");
	        Logger.writeLog("Printing BchSchlist");
	        for(int i=0;i<bchSch.curTime.size();i++){
	            Logger.writeLog(i+" "+bchSch.qryIdLst.get(i)+" "+bchSch.compStartTimeLst.get(i)+" "+bchSch.compStopTimeLst.get(i)+" "+bchSch.reqNumNodesLst.get(i));
	            //System.out.println(i+" "+bchSch.qryIdLst.get(i)+" "+bchSch.compStartTimeLst.get(i)+" "+bchSch.compStopTimeLst.get(i)+" "+bchSch.reqNumNodesLst.get(i));
	        }
	        //System.out.println("************************");
	        
	        //System.out.println(schPtValues.simuTime.get(schPtValues.simuTime.size()-1));
	        //Logger.writeLog("Printing schPtValues");
	        //for(int kk=schPtValues.simuTime.size()-5;kk<schPtValues.simuTime.size();kk++)
	           // Logger.writeLog(schPtValues.simuTime.get(kk)+" "+schPtValues.qryIdLst.get(kk).get(0));
	                    
	        if(bchSch.schGenerated){
	            
	            simuCurTime=bchSch.compStopTimeLst.get(bchSch.compStopTimeLst.size()-1);
	            exitFlg=true;
	            //System.out.println("************************");
	            /*System.out.println("Printing final list");
	            for(int i=0;i<bchSch.curTime.size();i++){
	                if(i==0 || i==bchSch.curTime.size()-1)
	                System.out.println(i+" "+bchSch.curTime.get(i)+" "+bchSch.qryIdLst.get(i)+" "+bchSch.compStartTimeLst.get(i)+" "+bchSch.compStopTimeLst.get(i)+" "+bchSch.reqNumNodesLst.get(i));
	                else if(bchSch.reqNumNodesLst.get(i)==bchSch.reqNumNodesLst.get(i+1))
	                    continue;
	                else{
	                    System.out.println(i+" "+bchSch.curTime.get(i)+" "+bchSch.qryIdLst.get(i)+" "+bchSch.compStartTimeLst.get(i)+" "+bchSch.compStopTimeLst.get(i)+" "+bchSch.reqNumNodesLst.get(i));
	                    System.out.println((i+1)+" "+bchSch.curTime.get(i+1)+" "+bchSch.qryIdLst.get(i+1)+" "+bchSch.compStartTimeLst.get(i+1)+" "+bchSch.compStopTimeLst.get(i+1)+" "+bchSch.reqNumNodesLst.get(i+1));
	                }
	                    
	            }
	            //System.out.println("************************");*/
	            
	        }
	        else if(!addNodes){            
	            exitFlg=true;
	        }
	        else{
	            
	            /*System.out.println(schPtValues.simuTime.get(schPtValues.simuTime.size()-1));
	            for(int kk=0;kk<simuQryList.size();kk++)
	                System.out.println(schPtValues.qryIdLst.get(schPtValues.simuTime.size()-1).get(kk)+" "+schPtValues.numTuplesProcessedLst.get(schPtValues.simuTime.size()-1).get(kk)+" "+schPtValues.completionStsLst.get(schPtValues.simuTime.size()-1).get(kk));*/
	                    
	            boolean resetIndex=false;
	            if(bchSch.curIndex==-1)
	                resetIndex=true;
	            else if(bchSch.curTime.get(bchSch.curTime.size()-1)>bchSch.maxTimeTBSched){
	                for(int i=bchSch.curTime.size()-1;i>bchSch.curIndex;i--){
	                    if(bchSch.compStartTimeLst.get(i)-bchSch.compStopTimeLst.get(i-1)>0){
	                        resetIndex=true;
	                        break;
	                    }
	                }
	                //if(!resetIndex)
	                  //  System.out.println("ToLog::Index not getting reset!!!!!!!!!!!!");
	            }
	            //if(bchSch.curIndex==-1 || bchSch.curTime.get(bchSch.curTime.size()-1)>bchSch.maxTimeTBSched){
	            if(resetIndex){
	                nodeIndex++;
	                if(nodeIndex>Cluster.numNodes.size()-1){
	                    //System.out.println("Sch not possible with current max nodes ");
	                    Logger.writeLog("Max node index reached-1...@ "+simuCurTime);	
	                    Logger.writeLog("Sch not possible with current max nodes "+simuCurTime+" ininodeindex"+startNodeIndex+" rsl/numTimes="+numTimes);
	                    exitFlg=true;
	                } 
	                      
	          
	            bchSch.curIndex=bchSch.curTime.size();
	            bchSch.maxTimeTBSched=bchSch.curTime.get(bchSch.curTime.size()-1);    
	            bchSch.qryIdTBS=bchSch.qryIdLst.get(bchSch.curTime.size()-1);
	            bchSch.qryBchNoTBS=bchSch.qryBatchNoLst.get(bchSch.curTime.size()-1);   
	            bchSch.minIndex=0;            
	            //finding minIndexPoint based on idle time
	            for(int i=bchSch.curTime.size()-1;i>0;i--){
	                if(bchSch.compStartTimeLst.get(i)-bchSch.compStopTimeLst.get(i-1)==0){
	                    bchSch.minIndex=i-1;
	                }
	                else
	                    break;
	            }
	            //System.out.println("Updated minIndexPoint::"+bchSch.minIndex+" maxIndexPoint::"+bchSch.curIndex+" maxTimeSched::"+bchSch.maxTimeSched);    
	            //Logger.writeLog("Updated minIndexPoint::"+bchSch.minIndex+" maxIndexPoint::"+bchSch.curIndex+" maxTimeSched::"+bchSch.maxTimeSched);                                               
	               
	            }
	            
	                
	            if((bchSch.curIndex-bchSch.minIndex)>100)
                        bchSch.curIndex-=10;
                    else	
                        bchSch.curIndex--;
	            //if(bchSch.curIndex<bchSch.minIndex || (bchSch.maxTimeTBSched-bchSch.curTime.get(bchSch.curIndex)>180)){
                    if(bchSch.curIndex<bchSch.minIndex){
	                bchSch.curIndex=bchSch.curTime.size()-1;
	                nodeIndex++;
	                if(nodeIndex>Cluster.numNodes.size()-1){
	                    //System.out.println("Sch not possible with current max nodes ");
	                    Logger.writeLog("Max node index reached-1...@ "+simuCurTime);	
	                    Logger.writeLog("Sch not possible with current max nodes "+simuCurTime+" ininodeindex"+startNodeIndex+" rsl/numTimes="+numTimes);
	                    exitFlg=true;
	                }
	                //reset nodeIndex for init value for all entries, last one gets removed subseq
	                for(int i=0;i<bchSch.curIndex;i++)
	                    bchSch.reqNumNodesLst.set(i,Cluster.numNodes.get(startNodeIndex));
	            } 
	            
	            
	           
	        }
	       
	    }
            if(bchSch.schGenerated){
	    //System.out.println("time @ end of simulation"+simuCurTime);
	    Logger.writeLog("ToLog::************************");
            Logger.writeLog("ToLog::Printing QryBchSchlist for INN:"+Cluster.numNodes.get(startNodeIndex)+" and BatchSize:"+numTimes+"X");
            Logger.writeLog("ToLog::Index QryID  BST   BET  Deadline NumNodes Result");
		        /*for(int i=0;i<bchSch.curTime.size();i++){
		            Logger.writeLog("ToLog::"+i+" "+bchSch.qryIdLst.get(i)+" "+bchSch.compStartTimeLst.get(i)+" "+bchSch.compStopTimeLst.get(i)+" "+bchSch.reqNumNodesLst.get(i));
		            //System.out.println(i+" "+bchSch.qryIdLst.get(i)+" "+bchSch.compStartTimeLst.get(i)+" "+bchSch.compStopTimeLst.get(i)+" "+bchSch.reqNumNodesLst.get(i));
		        }*/
            writeResToFile(bchSch,q_list);
	    Logger.writeLog("ToLog::************************");
            CompCostFrmNodeSch(bchSch,simuCurTime);            
            }
            else{
                Logger.writeLog("ToLog::************************");
                Logger.writeLog("ToLog::Schedule not possible for INN:"+Cluster.numNodes.get(startNodeIndex)+" and BatchSize:"+numTimes+"X");
                Logger.writeLog("ToLog::************************");
            }
            
	    
	    return bchSch;
	                
	}
	
	public static void CompCostFrmNodeSch(MultiQryBchSch multiQryBchSchLstTmp,float simuEndTime){
        float startTime=multiQryBchSchLstTmp.startTime;
        float stopTime=simuEndTime;
        int nodeIndexTmp=Cluster.getNumNodeIndex(multiQryBchSchLstTmp.reqNumNodesLst.get(0));
        

        for(int i=0;i<multiQryBchSchLstTmp.reqNumNodesLst.size();i++) {    
        	//System.out.println(i+" "+multiQryBchSchLstTmp.curTime.get(i)+" "+multiQryBchSchLstTmp.reqNumNodesLst.get(i));
        }
        int costCmpStartIndex=0;
        if(multiQryBchSchLstTmp.reqNumNodesLst.get(0)>Cluster.defaultNodeIndex)
            costCmpStartIndex=1;
        for(int i=costCmpStartIndex;i<multiQryBchSchLstTmp.reqNumNodesLst.size();i++) { 
            if(i==multiQryBchSchLstTmp.reqNumNodesLst.size()-1){
                //nodeIndexTmp=getNumNodeIndex(multiQryBchSchLstTmp.reqNumNodesLst.get(i));
                //startTime=multiQryBchSchLstTmp.curTime.get(i);
                stopTime=simuEndTime;
            }
            else if(Cluster.getNumNodeIndex(multiQryBchSchLstTmp.reqNumNodesLst.get(i))==nodeIndexTmp)
            	continue;
            else{
	    	stopTime=multiQryBchSchLstTmp.curTime.get(i);                		
	        multiQryBchSchLstTmp.totalCost+=(stopTime-startTime)*Cluster.costFact.get(nodeIndexTmp);
        	//System.out.println(startTime+"\t"+stopTime+"\t"+AWSCluster.costFact.get(nodeIndexTmp)+"\t"+multiQryBchSchLstTmp.totalCost+" "+i+" "+multiQryBchSchLstTmp.reqNumNodesLst.size());
	        if(i!=multiQryBchSchLstTmp.reqNumNodesLst.size()-1) {
        	        nodeIndexTmp=Cluster.getNumNodeIndex(multiQryBchSchLstTmp.reqNumNodesLst.get(i));
        	        startTime=multiQryBchSchLstTmp.curTime.get(i);                			
       		}
            	else {
                nodeIndexTmp=Cluster.getNumNodeIndex(multiQryBchSchLstTmp.reqNumNodesLst.get(i));
                startTime=multiQryBchSchLstTmp.curTime.get(i);
                stopTime=simuEndTime;
            	}   		
            }
        }
        //System.out.println("multiQryBchSchLstTmp.totalCost="+multiQryBchSchLstTmp.totalCost);        
        if(nodeIndexTmp==0) {
           		multiQryBchSchLstTmp.totalCost+=(stopTime-startTime)*Cluster.costFact.get(nodeIndexTmp);
        }
        else {
           		multiQryBchSchLstTmp.totalCost+=(stopTime-startTime+Cluster.clusterInitOH)*Cluster.costFact.get(nodeIndexTmp);
        }
        //System.out.println(startTime+"\t"+stopTime+"\t"+Cluster.costFact.get(nodeIndexTmp)+"\t"+multiQryBchSchLstTmp.totalCost+"\t"+nodeIndexTmp);
        /*else {
        		VerifyValues(numNodesTmp,AWSCluster.numNodes.get(AWSCluster.numNodes.size()-1),6);
           		int excessNodes=numNodesTmp-AWSCluster.numNodes.get(AWSCluster.numNodes.size()-1);
           		multiQrySchLstTmp.totalCost+=(stopTime-startTime+AWSCluster.clusterInitOH)*(AWSCluster.costFact.get(nodeIndexTmp)+(excessNodes*AWSCluster.excessNodesCostFact));
        }*/
            		
       //System.out.println(startTime+"\t"+stopTime+"\t"+AWSCluster.costFact.get(nodeIndexTmp)+"\t"+multiQrySchLstTmp.totalCost);
        //QryProcLstForSimu qryProcLstTmp2=qryProcLst.get(qryProcLst.size()-1);
        //System.out.println(qryProcLstTmp2.qryId+" "+qryProcLstTmp2.compStartTime+" "+qryProcLstTmp2.compStopTime+" numNodes===="+qryProcLstTmp2.numNodes+" rsl/numTimes"+numTimes+" startNodes="+startNumIndex);
	Logger.writeLog("ToLog::Total Cost in Dollars::"+multiQryBchSchLstTmp.totalCost);
        //System.out.println("******************* Cost. Sch length="+multiQryBchSchLstTmp.reqNumNodesLst.size()+" cost="+multiQryBchSchLstTmp.totalCost);
       //System.out.println("Computing Cost. Sch length="+multiQrySchLstTmp.reqNumNodesLst.size()+" cost="+multiQrySchLstTmp.totalCost);
       //Logger.writeLog("Computing Cost. Sch length="+multiQrySchLstTmp.reqNumNodesLst.size()+" cost="+multiQrySchLstTmp.totalCost);
	   //Logger.writeLog("End of Processing with Config::"+startNumIndex+" CurTime::"+simuCurTime);
	   
                
       /*Logger.writeLog("Printing multiQrySchLstTmp");
       for(int i=0;i<multiQrySchLstTmp.reqNumNodesLst.size();i++) 
          	Logger.writeLog("Printing Sch::"+i+"\t"+multiQrySchLstTmp.nodeReqTimeLst.get(i)+"\t"+multiQrySchLstTmp.reqNumNodesLst.get(i));*/
      
}
	
	public static void genFixedConfigSchMain(float cur_time, List<Query> q_list){
	    
	    int[] numTimesLst={1,2,4,8,16,32};
	    for(int c=0;c<5;c++) {
                       
                      
		for(int x=0;x<numTimesLst.length;x++){ //batch size 1 to 32
		    
		    int nodeIndex=c;
		    int numTimes=numTimesLst[x];
		    
		    
		  
		    //int bchSchIndex=-1;
		    
		    float simuCurTime=cur_time;
		    float simuStartTime=cur_time;
		    //float totalComputeTime=0.0f;
		    ArrayList<Query> simuQryList=new ArrayList<Query>();
		    ArrayList<Query> simuQryList2=new ArrayList<Query>();
		    ArrayList<Query> simuQryListTmp=new ArrayList<Query>();
		    
		 
		        
		    /*NodeSchedule nodeSch=new NodeSchedule();
		    nodeSch.time.add(simuStartTime);
		    nodeSch.numNodeIndex.add(startNodeIndex);*/
		    
		    MultiQryBchSch bchSch = new MultiQryBchSch();
		   
		        
		     
		    simuQryList.clear();
		    simuQryList2.clear();
		    simuQryListTmp.clear();
		            
		    float minWindStartTime=9999.0f;
		    for(int i=0;i<q_list.size();i++) {			
			if(q_list.get(i).completed==false){
		            if(q_list.get(i).wind_start_time<minWindStartTime) {
				minWindStartTime=q_list.get(i).wind_start_time;
		                Logger.writeLog("simuCurTime updated to "+simuCurTime+" from "+simuCurTime);
		            }
		        }
		    }
		    if(simuCurTime<minWindStartTime)
		        simuCurTime=minWindStartTime;
		    //Logger.writeLog("simuCurTime updated to "+simuCurTime);
		    
		    bchSch.startTime=simuCurTime;
		    
		   
		   
		        
			for(int i=0;i<q_list.size();i++) {	
		           
		            if(q_list.get(i).completed==false){	
		                Query tmpQry=new Query(q_list.get(i));
				//System.out.println(this.q_list.get(i).query_id+" "+q_list.get(i).num_tuple_total);				
				tmpQry.simuPars.numTuplesProcessed=q_list.get(i).num_tuple_processed;
		             
				int extraTuples=0;
				if(simuCurTime<q_list.get(i).wind_end_time && simuCurTime>=q_list.get(i).wind_start_time) {				
		                    extraTuples=(int)((q_list.get(i).wind_end_time-simuCurTime)*(q_list.get(i).input_rate));
		                    tmpQry.num_tuple_total=(int)(tmpQry.compFileNoWithIpRate(simuCurTime)+extraTuples-tmpQry.compFileNoWithIpRate(tmpQry.wind_start_time-1));
		                   
				}
				else if(simuCurTime<q_list.get(i).wind_end_time && simuCurTime<q_list.get(i).wind_start_time) {				
		                    //tmpQry.num_tuple_total=(int)(tmpQry.compFileNoWithIpRate(tmpQry.wind_end_time)-tmpQry.compFileNoWithIpRate(tmpQry.wind_start_time-1));
		                    //Logger.writeLog(tmpQry.compFileNoWithIpRate(tmpQry.wind_end_time)+" "+tmpQry.compFileNoWithIpRate(tmpQry.wind_start_time-1)+" 22 ");
		                    tmpQry.num_tuple_total=(int)((q_list.get(i).wind_end_time-q_list.get(i).wind_start_time+1)*q_list.get(i).input_rate);
		                   
				}
		                else{
		                    tmpQry.num_tuple_total=(int)(tmpQry.compFileNoWithIpRate(simuCurTime)-tmpQry.compFileNoWithIpRate(tmpQry.wind_start_time-1));
		                    // Logger.writeLog(tmpQry.compFileNoWithIpRate(simuCurTime)+" "+tmpQry.compFileNoWithIpRate(tmpQry.wind_start_time-1)+" 33 ");
		                   
		                }
		                                
							
				//System.out.println(cur_time+" "+tmpQry.compFileNoWithIpRate(simuCurTime)+" "+extraTuples+" "+tmpQry.num_tuple_total+"  for checkingggg");
				
				tmpQry.simuPars.numTuplesPending=tmpQry.num_tuple_total-tmpQry.simuPars.numTuplesProcessed;
				tmpQry.simuPars.curBatchNo=q_list.get(i).cur_batch_no+1;
				tmpQry.simuPars.numBatchesProcessed=q_list.get(i).cur_batch_no;	
				//tmpQry.simuPars.numBatchesForAgg=q_list.get(i).numBatchesForAgg;
				//tmpQry.simuPars.curNumNodes=AWSCluster.numNodes.get(nodeIndex);
				//System.out.println(simuQryList.get(i).query_id+" "+simuQryList.get(i).simuPars.numTuplesPending);
		              
		                
				simuQryList.add(tmpQry);
		                simuQryList2.add(tmpQry);  
		                simuQryListTmp.add(tmpQry);
				
		            }
			}		
			
		        for(int i=0;i<simuQryList.size();i++) {
		            //System.out.println(simuQryList.get(i).query_id+" "+q_list.get(i).num_tuple_total);			
		            //simuQryList.get(i).detMinBatchSizeForMultiQryRSLBased(nodeIndex,rslFact);	
		            
		            //simuQryList.get(i).detMinBatchSizeForMultiQryIpRateBased(numTimes,nodeIndex);
		            simuQryList.get(i).detMinBatchSizeForMultiQryIpRateBased(numTimes,nodeIndex);
		           
		            
		            if((simuQryList.get(i).num_tuple_total-simuQryList.get(i).num_tuple_processed)>simuQryList.get(i).dynCaseMinBatchSize)
		                simuQryList.get(i).simuPars.curBatchSize=simuQryList.get(i).dynCaseMinBatchSize;
		            else
				simuQryList.get(i).simuPars.curBatchSize=(simuQryList.get(i).num_tuple_total-simuQryList.get(i).num_tuple_processed);
			
		           
		            
		            if(Cluster.numBatchPcntForParAgg==1.0f){
		                simuQryList.get(i).simuPars.numBatchesForAgg=(int)((simuQryList.get(i).num_tuple_total/simuQryList.get(i).simuPars.curBatchSize));
		            if(simuQryList.get(i).num_tuple_total%simuQryList.get(i).simuPars.curBatchSize>0)
		                simuQryList.get(i).simuPars.numBatchesForAgg++;
		            }
		            else{
		                simuQryList.get(i).simuPars.numBatchesForAgg=(int)((simuQryList.get(i).num_tuple_total/simuQryList.get(i).simuPars.curBatchSize)*Cluster.numBatchPcntForParAgg);
				if(simuQryList.get(i).simuPars.numBatchesForAgg==0) {
		                    simuQryList.get(i).simuPars.numBatchesForAgg=simuQryList.get(i).num_tuple_total/simuQryList.get(i).simuPars.curBatchSize;
				if(simuQryList.get(i).num_tuple_total%simuQryList.get(i).simuPars.curBatchSize>0)
		                    simuQryList.get(i).simuPars.numBatchesForAgg++;
				}
		            }
			
		           
		            //System.out.println(simuQryList.get(i).num_tuple_total+" "+simuQryList.get(i).simuPars.curBatchSize+" "+AWSCluster.numBatchPcntForParAgg);
		            //System.out.println(simuQryList.get(i).query_id+" "+simuQryList.get(i).num_tuple_total+" "+simuQryList.get(i).simuPars.numTuplesProcessed+" "+q_list.get(i).num_tuple_total+" "+q_list.get(i).simuPars.numBatchesForAgg+" "+simuQryList.get(i).simuPars.curBatchSize+" "+AWSCluster.numBatchPcntForParAgg);
		            /*if(simuQryList.get(i).simuPars.numBatchesForAgg==0) {
		                System.out.println("simuQryList.get(i).simuPars.numBatchesForAgg became zero");
				//System.exit(-1);
		            }*/
		            
		           
					 
			}
			
			
		        

		        
		        //System.out.println(bchSch.curIndex+" "+schPtValues.simuTime.size());
			if(simuCurTime<minWindStartTime)
		            simuCurTime=minWindStartTime;
		        
		        
		        //System.out.println("simuCurTime is::"+simuCurTime);
		       /* simuQryListTmp.clear();
		        for(int i=0;i<this.q_list.size();i++) {			
		            if(this.q_list.get(i).completed==false){	
		                query tmpQry=new query(this.q_list.get(i));
		                simuQryListTmp.add(tmpQry);
		            }
		        }*/
		            
		      
		     //minconfig run
		        genFixedConfigSch(simuQryListTmp, simuCurTime,  bchSch, nodeIndex);
		        Logger.writeLog("ToLog::************************");
		        Logger.writeLog("ToLog::Printing QryBchSchlist for "+Cluster.numNodes.get(nodeIndex)+" nodes and BatchSize:"+numTimes+"X");
			Logger.writeLog("ToLog::Index QryID  BST   BET  Deadline NumNodes Result");
		        /*for(int i=0;i<bchSch.curTime.size();i++){
		            Logger.writeLog("ToLog::"+i+" "+bchSch.qryIdLst.get(i)+" "+bchSch.compStartTimeLst.get(i)+" "+bchSch.compStopTimeLst.get(i)+" "+bchSch.reqNumNodesLst.get(i));
		            //System.out.println(i+" "+bchSch.qryIdLst.get(i)+" "+bchSch.compStartTimeLst.get(i)+" "+bchSch.compStopTimeLst.get(i)+" "+bchSch.reqNumNodesLst.get(i));
		        }*/
                        writeResToFile(bchSch,q_list);
		        Logger.writeLog("ToLog::************************");
		        CompCostFrmNodeSch(bchSch,bchSch.compStopTimeLst.get(bchSch.compStopTimeLst.size()-1));
		   }
            }
	}
public static void genFixedConfigSch(ArrayList<Query> simuQryListTmp, float simuCurTime, MultiQryBchSch bchSch, int nodeIndex){
		   
		    bchSch.schGenerated = true;
		    float startSimuCurTime=simuCurTime;
		  
		    
		    for(int i=0;i<simuQryListTmp.size();i++) 
		            Logger.writeLog("checkPars:"+simuQryListTmp.get(i).query_id+" "+simuQryListTmp.get(i).simuPars.numTuplesPending+" "+simuQryListTmp.get(i).simuPars.curBatchReadyTime+" "+simuQryListTmp.get(i).simuPars.curBatchNo);
		    
		    boolean exitFlg=false;
		    while(!exitFlg) {
		        
		      
		    
			
		       
			Logger.writeLog("CurTime::"+simuCurTime+" nodeIndex="+nodeIndex+" estNumNodesTmp="+Cluster.numNodes.get(nodeIndex));
			
		        //check if all queries completed
		        boolean allCompleted=true;
		        for(int i=0;i<simuQryListTmp.size();i++) {
		            if(simuQryListTmp.get(i).completed==false){
		                allCompleted=false;
		                break;
		            }
		        }
		        if(allCompleted){
		            bchSch.schGenerated=true;
		            exitFlg=true;
		            break;
		        }
		       
		        
		      
			//System.out.println("estNumNodesTmp="+estNumNodesTmp);
			for(int i=0;i<simuQryListTmp.size();i++) { 
		            //System.out.println(simuQryListTmp.get(i).query_id+" "+simuQryListTmp.get(i).completed);
		            if(simuQryListTmp.get(i).completed==false){                
		            //Logger.writeLog("checkPars:"+simuCurTime+" "+simuQryListTmp.get(i).query_id+" "+simuQryListTmp.get(i).simuPars.numTuplesPending+" "+simuQryListTmp.get(i).simuPars.curBatchReadyTime);
		            if(simuCurTime>simuQryListTmp.get(i).wind_end_time) {
		              	simuQryListTmp.get(i).simuPars.curBatchReadyTime=simuQryListTmp.get(i).wind_end_time;
		                int tmpTuples=simuQryListTmp.get(i).EstTuples(nodeIndex,Cluster.cmax);
		                if(tmpTuples>simuQryListTmp.get(i).simuPars.numTuplesPending) {
		                    simuQryListTmp.get(i).simuPars.curBatchSize=simuQryListTmp.get(i).simuPars.numTuplesPending;
		                }
		                else {
		                    simuQryListTmp.get(i).simuPars.curBatchSize=tmpTuples;
		                }                                	
		                }                                    
		                else {
		                if(simuQryListTmp.get(i).simuPars.numTuplesPending<simuQryListTmp.get(i).simuPars.curBatchSize)
		                		simuQryListTmp.get(i).simuPars.curBatchSize=simuQryListTmp.get(i).simuPars.numTuplesPending;
		                 simuQryListTmp.get(i).simuPars.curBatchReadyTime=simuQryListTmp.get(i).ip_time_modified(simuQryListTmp.get(i).simuPars.numTuplesProcessed+
							simuQryListTmp.get(i).simuPars.curBatchSize,simuQryListTmp.get(i).input_rate);
		                }
		                }
		         }
		         
		            
		        
		            
		         for(int i=0;i<simuQryListTmp.size();i++){
		         if(simuQryListTmp.get(i).completed==false){
		         	//System.out.println("nodeindexxxx is "+nodeIndex+" @ "+simuCurTime);                    
		                int numBatchesPending=(int)(simuQryListTmp.get(i).simuPars.numTuplesPending/simuQryListTmp.get(i).simuPars.curBatchSize);
				simuQryListTmp.get(i).simuPars.curBatchCompTime=simuQryListTmp.get(i).EstDuration(nodeIndex,simuQryListTmp.get(i).simuPars.curBatchSize);
				//Logger.writeLog("curBatchCompTime before and after::"+simuQryList.get(i).query_id+" "+simuQryList.get(i).simuPars.curBatchCompTime+" "+simuQryList.get(i).simuPars.numBatchesForAgg+" "+simuQryList.get(i).simuPars.curBatchNo+" "+simuQryList.get(i).simuPars.curBatchSize);
				
				simuQryListTmp.get(i).simuPars.curBatchAggTime=simuQryListTmp.get(i).compAggCost(nodeIndex,simuQryListTmp.get(i).simuPars.numBatchesForAgg);
						
				//Logger.writeLog("curBatchCompTime before and after::"+simuQryList.get(i).query_id+" "+simuQryList.get(i).simuPars.curBatchAggTime+" "+numBatchesPending);
				simuQryListTmp.get(i).simuPars.totalCompTime=numBatchesPending*simuQryListTmp.get(i).simuPars.curBatchCompTime;
				//Logger.writeLog("1:"+simuQryList.get(i).query_id+" "+simuQryList.get(i).simuPars.totalCompTime);
				if(simuQryListTmp.get(i).simuPars.numTuplesPending%simuQryListTmp.get(i).simuPars.curBatchSize>0) {	
					//Logger.writeLog("ForVerifyValues:"+simuQryList.get(i).query_id+" "+simuCurTime+" "+simuQryList.get(i).num_tuple_total+" numBatchesProcessed="+simuQryList.get(i).simuPars.numBatchesProcessed+
					//	" numBatchesPending="+numBatchesPending+" curBatchSize="+simuQryList.get(i).simuPars.curBatchSize+" numTuplesPending="+simuQryList.get(i).simuPars.numTuplesPending+" "+(simuQryList.get(i).simuPars.numTuplesPending%simuQryList.get(i).simuPars.curBatchSize));
					//VerifyValues(simuQryList.get(i).num_tuple_total,((simuQryList.get(i).simuPars.numBatchesProcessed+numBatchesPending-1)*simuQryList.get(i).simuPars.curBatchSize),8);
					simuQryListTmp.get(i).simuPars.totalCompTime+=simuQryListTmp.get(i).EstDuration(nodeIndex,(simuQryListTmp.get(i).simuPars.numTuplesPending%simuQryListTmp.get(i).simuPars.curBatchSize));							
					numBatchesPending++;		
					//Logger.writeLog("2:"+simuQryList.get(i).query_id+" "+simuQryList.get(i).simuPars.totalCompTime+" "+simuQryList.get(i).simuPars.numTuplesPending+" "+simuQryList.get(i).simuPars.curBatchSize);
				}
				
		                //System.out.println("totalCompTime-2 "+simuQryList.get(i).query_id+" "+simuQryList.get(i).simuPars.totalCompTime+" "+simuQryList.get(i).simuPars.curBatchAggTime+" batchesForAgg::"+simuQryList.get(i).simuPars.numBatchesForAgg+" "+numBatchesPending);
		                if(Cluster.numBatchPcntForParAgg!=1.0f)                                    
		                    simuQryListTmp.get(i).simuPars.totalCompTime+=(numBatchesPending/simuQryListTmp.get(i).simuPars.numBatchesForAgg)*simuQryListTmp.get(i).simuPars.curBatchAggTime;
		                            
		                    //System.out.println("check this::"+((numBatchesPending/simuQryList.get(i).simuPars.numBatchesForAgg)*simuQryList.get(i).simuPars.curBatchAggTime));    
		                if(Cluster.numBatchPcntForParAgg==1.0f)
		                    simuQryListTmp.get(i).simuPars.aggTime=simuQryListTmp.get(i).compAggCost(nodeIndex,((numBatchesPending+simuQryListTmp.get(i).simuPars.numBatchesProcessed-1)));
				else
		                    simuQryListTmp.get(i).simuPars.aggTime=simuQryListTmp.get(i).compAggCost(nodeIndex,((numBatchesPending+simuQryListTmp.get(i).simuPars.numBatchesProcessed-1)/simuQryListTmp.get(i).simuPars.numBatchesForAgg));	
						//simuQryList.get(i).simuPars.aggTime=simuQryList.get(i).compAggCost(nodeIndex,(numBatchesPending+simuQryList.get(i).simuPars.numBatchesProcessed-1));
				/*System.out.println(simuQryList.get(i).query_id+" Batch No:"+simuQryList.get(i).simuPars.numBatchesProcessed+" "+simuQryList.get(i).simuPars.curBatchReadyTime+
								" "+simuQryList.get(i).simuPars.curBatchCompTime+" "+simuQryList.get(i).simuPars.totalCompTime);*/
				simuQryListTmp.get(i).simuPars.totalCompTime+=simuQryListTmp.get(i).simuPars.aggTime;
				//System.out.println("totalCompTime-3 "+simuQryList.get(i).query_id+" "+simuQryList.get(i).simuPars.totalCompTime+" "+simuQryList.get(i).simuPars.aggTime);
				//Logger.writeLog("3:"+simuQryList.get(i).query_id+" "+simuQryList.get(i).simuPars.totalCompTime+" "+simuQryList.get(i).simuPars.aggTime+" "+simuQryList.get(i).simuPars.curBatchCompTime);
				//Logger.writeLog("curBatchCompTime before and after::"+simuQryList.get(i).query_id+" "+simuQryList.get(i).query_id+" "+simuQryList.get(i).simuPars.totalCompTime+" "+simuQryList.get(i).simuPars.aggTime+" "+simuQryList.get(i).simuPars.curBatchCompTime+" "+numBatchesPending+" "+simuQryList.get(i).simuPars.numBatchesProcessed);
				float compStartTimeTmp;
				if(simuCurTime>simuQryListTmp.get(i).simuPars.curBatchReadyTime)
		                    compStartTimeTmp=simuCurTime;
				else
		                    compStartTimeTmp=simuQryListTmp.get(i).simuPars.curBatchReadyTime;
						
						
						
				simuQryListTmp.get(i).simuPars.slackTime=simuQryListTmp.get(i).compSlackTimeGeneric(simuQryListTmp.get(i).deadline,compStartTimeTmp, simuQryListTmp.get(i).simuPars.totalCompTime);				
				Logger.writeLog(simuQryListTmp.get(i).query_id+" Batch No:"+simuQryListTmp.get(i).simuPars.numBatchesProcessed+" "+simuQryListTmp.get(i).simuPars.curBatchReadyTime+
								" "+simuQryListTmp.get(i).simuPars.curBatchCompTime+" "+simuQryListTmp.get(i).simuPars.totalCompTime+" "+
								simuQryListTmp.get(i).simuPars.numBatchesProcessed+" "+numBatchesPending+" curBchSize="+simuQryListTmp.get(i).simuPars.curBatchSize+" "+simuQryListTmp.get(i).simuPars.numBatchesForAgg+" "+simuQryListTmp.get(i).simuPars.aggTime);
				/*System.out.println(simuQryList.get(i).deadline+" "+
							simuQryList.get(i).simuPars.curBatchReadyTime+" "+simuQryList.get(i).simuPars.totalCompTime+" "+simuQryList.get(i).simuPars.curBatchSize);*/
				/*Logger.writeLog(simuQryListTmp.get(i).query_id+" slackTime="+simuQryListTmp.get(i).simuPars.slackTime+" compStartTime="+
								compStartTime+" totalCompTime="+simuQryListTmp.get(i).simuPars.totalCompTime+" curBchCompTime"+simuQryListTmp.get(i).simuPars.curBatchCompTime
								+" BRT:"+simuQryListTmp.get(i).simuPars.curBatchReadyTime+" deadline="+simuQryListTmp.get(i).deadline);*/
		            
		             
		                //completionSts.add(i,simuQryListTmp.get(i).completed);
		                //qryId.add(i,simuQryListTmp.get(i).query_id);
		            }
		            else{
		                //completionSts.add(i,true);
		                //qryId.add(i,simuQryListTmp.get(i).query_id);
		            }
		            }
					
		           
		            
		            /*System.out.println("priting from SchPtValues::"+schPtValues.simuTime.get(schPtValues.simuTime.size()-1));
		            for(int kk=0;kk<simuQryListTmp.size();kk++)
		                System.out.println(schPtValues.qryIdLst.get(schPtValues.simuTime.size()-1).get(kk)+" "+schPtValues.numTuplesProcessedLst.get(schPtValues.simuTime.size()-1).get(kk)+" "+schPtValues.completionStsLst.get(schPtValues.simuTime.size()-1).get(kk)+" "+completionSts.get(kk));*/
		            
		            //sort queries
		            float minSlackTime=9999.0f;
		            float minBatchReadyTime=9999.0f;
		            int selQryIndex=-1;
					
		            
		            /*for(int i=0;i<simuQryListTmp.size();i++) {
				//System.out.println(simuQryList.get(i).query_id+" "+simuQryList.get(i).simuPars.curBatchReadyTime+" "+simuCurTime);
				if(simuQryListTmp.get(i).completed==false && simuQryListTmp.get(i).simuPars.slackTime<0) {                    
		                        minSlackTime=simuQryListTmp.get(i).simuPars.slackTime;
					selQryIndex=i;
		                        break;                   
				}
		            }*/
		            
		            if(selQryIndex==-1){
		            for(int i=0;i<simuQryListTmp.size();i++) {
				//System.out.println(simuQryList.get(i).query_id+" "+simuQryList.get(i).simuPars.curBatchReadyTime+" "+simuCurTime);
				if(simuQryListTmp.get(i).completed==false && simuQryListTmp.get(i).simuPars.curBatchReadyTime<=simuCurTime) {
		                    if(simuQryListTmp.get(i).simuPars.slackTime<minSlackTime) {
		                        minSlackTime=simuQryListTmp.get(i).simuPars.slackTime;
					selQryIndex=i;
		                    }
				}
		            }
		            }
		            
		            
			
		           
		            //System.out.println(selQryIndex+" "+minSlackTime);
		            if(selQryIndex!=-1) {
				//System.out.println("---------------------checkkkkkkkkkkkkkkkkkkkkkkkkk"+simuQryList.get(selQryIndex).query_id+" "+simuQryList.get(selQryIndex).simuPars.curBatchReadyTime+" "+simuCurTime);			
				//Logger.writeLog("selQry="+selQryIndex+" @ "+simuCurTime+" is "+simuQryListTmp.get(selQryIndex).query_id+" "+minSlackTime+" "+minBatchReadyTime);
		            }
						
		            if(selQryIndex==-1) {
		                minBatchReadyTime=9999.0f;
				minSlackTime=9999.0f;
				for(int i=0;i<simuQryListTmp.size();i++) {
				if(simuQryListTmp.get(i).completed==false && simuQryListTmp.get(i).simuPars.curBatchReadyTime<=minBatchReadyTime) {
		                    minBatchReadyTime=simuQryListTmp.get(i).simuPars.curBatchReadyTime;
				}
				}
				for(int i=0;i<simuQryListTmp.size();i++) {
		                    if(simuQryListTmp.get(i).completed==false && simuQryListTmp.get(i).simuPars.curBatchReadyTime==minBatchReadyTime) {				
					if(simuQryListTmp.get(i).simuPars.slackTime<minSlackTime) {
		                            minSlackTime=simuQryListTmp.get(i).simuPars.slackTime;
		                            selQryIndex=i;
					}
		                    }
				}
		            }
			
		            //System.out.println("selQry="+selQryIndex+" "+simuQryListTmp.get(selQryIndex).query_id);
		            //Logger.writeLog("selQry="+selQryIndex+" @ "+simuCurTime+" is "+simuQryList.get(selQryIndex).query_id+" "+minSlackTime+" "+minBatchReadyTime);
					
		       
		            //Adding to qryProcLstTmp
		            /*ArrayList<query> curSimuQList=new ArrayList<>();
		            for(int i=0;i<simuQryListTmp.size();i++)
		                curSimuQList.add(new query(simuQryListTmp.get(i)));*/
		           
		            
		        
		            if(simuQryListTmp.get(selQryIndex).simuPars.curBatchReadyTime>simuCurTime) {	
		            //Logger.writeLog("curBatchReadyTime="+simuQryList.get(selQryIndex).simuPars.curBatchReadyTime+" curBatchCompTime="+simuQryList.get(selQryIndex).simuPars.curBatchCompTime+" "+simuCurTime+" "+aggDur[nodeIndex]);
		            simuCurTime=simuQryListTmp.get(selQryIndex).simuPars.curBatchReadyTime+simuQryListTmp.get(selQryIndex).simuPars.curBatchCompTime;
		            if(Cluster.numBatchPcntForParAgg==1.0f && simuQryListTmp.get(selQryIndex).cur_batch_no>=simuQryListTmp.get(selQryIndex).simuPars.numBatchesForAgg)
		                simuCurTime+=simuQryListTmp.get(selQryIndex).simuPars.curBatchAggTime;
		            else if(Cluster.numBatchPcntForParAgg!=1.0f && simuQryListTmp.get(selQryIndex).cur_batch_no>=simuQryListTmp.get(selQryIndex).simuPars.numBatchesForAgg && (simuQryListTmp.get(selQryIndex).simuPars.curBatchNo%simuQryListTmp.get(selQryIndex).simuPars.numBatchesForAgg==0))
				simuCurTime+=simuQryListTmp.get(selQryIndex).simuPars.curBatchAggTime;
		            
		            //QryProcLstForSimuNew(float curSimuTime, String selQryId, float compStartTime, float compStopTime, int numNodes, float batchRdyTime, int curBatchNo,ArrayList<Query> curSimuQList)
		            
		            //bchSch.curTime.add(simuQryListTmp.get(selQryIndex).simuPars.curBatchReadyTime);
		            if(bchSch.curTime.size()==0)
		                bchSch.curTime.add(startSimuCurTime);
		            else
		                bchSch.curTime.add(bchSch.compStopTimeLst.get(bchSch.compStopTimeLst.size()-1));
		            bchSch.qryIdLst.add(simuQryListTmp.get(selQryIndex).query_id);
		            bchSch.compStartTimeLst.add(simuQryListTmp.get(selQryIndex).simuPars.curBatchReadyTime);
		            bchSch.compStopTimeLst.add(simuCurTime);
		            bchSch.reqNumNodesLst.add(Cluster.numNodes.get(nodeIndex));
			    bchSch.batchRdyTimeLst.add(simuQryListTmp.get(selQryIndex).simuPars.curBatchReadyTime);
		            bchSch.qryBatchNoLst.add(simuQryListTmp.get(selQryIndex).simuPars.curBatchNo);
		        
		      
		            
		            }
		            else{		
		            
		           
		            float startSimuTime=simuCurTime;
		            //Logger.writeLog("simuCurTime="+simuCurTime+" curBatchCompTime="+simuQryList.get(selQryIndex).simuPars.curBatchCompTime);
		            simuCurTime=simuCurTime+simuQryListTmp.get(selQryIndex).simuPars.curBatchCompTime;
		            if(Cluster.numBatchPcntForParAgg==1.0f && simuQryListTmp.get(selQryIndex).cur_batch_no>=simuQryListTmp.get(selQryIndex).simuPars.numBatchesForAgg)
		                simuCurTime+=simuQryListTmp.get(selQryIndex).simuPars.curBatchAggTime;
		            else if(Cluster.numBatchPcntForParAgg!=1.0f && simuQryListTmp.get(selQryIndex).cur_batch_no>=simuQryListTmp.get(selQryIndex).simuPars.numBatchesForAgg && (simuQryListTmp.get(selQryIndex).simuPars.curBatchNo%simuQryListTmp.get(selQryIndex).simuPars.numBatchesForAgg==0))
		        	simuCurTime+=simuQryListTmp.get(selQryIndex).simuPars.curBatchAggTime;
		            //Logger.writeLog("From qryProcLst: size="+qryProcLst.size()+" "+simuCurTime+" "+simuQryList.get(selQryIndex).query_id);
		            
		            //bchSch.curTime.add(startSimuTime);
		            if(bchSch.curTime.size()==0)
		                bchSch.curTime.add(startSimuCurTime);
		            else
		                bchSch.curTime.add(bchSch.compStopTimeLst.get(bchSch.compStopTimeLst.size()-1));
		            bchSch.qryIdLst.add(simuQryListTmp.get(selQryIndex).query_id);
		            bchSch.compStartTimeLst.add(startSimuTime);
		            bchSch.compStopTimeLst.add(simuCurTime);
		            bchSch.reqNumNodesLst.add(Cluster.numNodes.get(nodeIndex));
			    bchSch.batchRdyTimeLst.add(simuQryListTmp.get(selQryIndex).simuPars.curBatchReadyTime);
		            bchSch.qryBatchNoLst.add(simuQryListTmp.get(selQryIndex).simuPars.curBatchNo);      
		            
		            
		            }
		            //Logger.writeLog("qryProcLst.size()2=="+qryProcLst.size());
		            //totalComputeTime+=simuQryList.get(selQryIndex).simuPars.curBatchCompTime;
					
		            /*System.out.println(simuQryList.get(selQryIndex).query_id+" Batch:"+simuQryList.get(selQryIndex).simuPars.numBatchesProcessed
				+" processed @ "+simuCurTime+" with Config="+nodeIndex+" curBatchCompTime="+simuQryList.get(selQryIndex).simuPars.curBatchCompTime+" aggCost="+
				simuQryList.get(selQryIndex).simuPars.aggTime+" totalTuples="+simuQryList.get(selQryIndex).num_tuple_total);*/
		            //Logger.writeLog(simuQryList.get(selQryIndex).query_id+" Batch:"+simuQryList.get(selQryIndex).simuPars.numBatchesProcessed+" processed @ "+simuCurTime+" with Config="+nodeIndex+" curBatchCompTime="+simuQryList.get(selQryIndex).simuPars.curBatchCompTime+" aggCost="+
		            //	simuQryList.get(selQryIndex).simuPars.aggTime+" totalTuples="+simuQryList.get(selQryIndex).num_tuple_total+" processed="+simuQryList.get(selQryIndex).simuPars.numTuplesProcessed+" curbchsize="+simuQryList.get(selQryIndex).simuPars.curBatchSize);
		            Logger.writeLog("Processssed "+simuQryListTmp.get(selQryIndex).query_id+" "+simuQryListTmp.get(selQryIndex).simuPars.curBatchReadyTime+" "+simuQryListTmp.get(selQryIndex).simuPars.curBatchCompTime+" "+simuCurTime+" "+simuQryListTmp.get(selQryIndex).simuPars.numTuplesPending+" "+simuQryListTmp.get(selQryIndex).simuPars.numTuplesProcessed+" "+simuQryListTmp.get(selQryIndex).simuPars.curBatchSize+" "+Cluster.numNodes.get(nodeIndex));
		            Logger.writeLog("bchSch size="+bchSch.curTime.size());
		            simuQryListTmp.get(selQryIndex).simuPars.curNumNodes=Cluster.numNodes.get(nodeIndex);
		            simuQryListTmp.get(selQryIndex).simuPars.numBatchesProcessed++;
		            simuQryListTmp.get(selQryIndex).simuPars.curBatchNo++;
		            simuQryListTmp.get(selQryIndex).simuPars.numTuplesProcessed+=simuQryListTmp.get(selQryIndex).simuPars.curBatchSize;							
		            if(simuQryListTmp.get(selQryIndex).simuPars.numTuplesPending<simuQryListTmp.get(selQryIndex).simuPars.curBatchSize) {
				simuQryListTmp.get(selQryIndex).simuPars.curBatchSize=simuQryListTmp.get(selQryIndex).simuPars.numTuplesPending;
				simuQryListTmp.get(selQryIndex).simuPars.numTuplesPending=0;
		            }
		            else {
		                //VerifyValues(simuQryListTmp.get(selQryIndex).simuPars.numTuplesPending,simuQryListTmp.get(selQryIndex).simuPars.curBatchSize,5);
				simuQryListTmp.get(selQryIndex).simuPars.numTuplesPending-=simuQryListTmp.get(selQryIndex).simuPars.curBatchSize;
		            }
		            //System.out.println(simuQryListTmp.get(selQryIndex).query_id+" numTuplesPending=="+simuQryListTmp.get(selQryIndex).simuPars.numTuplesPending+" "+simuQryListTmp.get(selQryIndex).num_tuple_total);
		            //Logger.writeLog("simuCurTime=="+simuCurTime);
		            
		            
		            
		            
		            if(simuQryListTmp.get(selQryIndex).simuPars.numTuplesProcessed>=simuQryListTmp.get(selQryIndex).num_tuple_total) {
		                Logger.writeLog("doing final agg for "+simuQryListTmp.get(selQryIndex).query_id+" @ "+simuCurTime+" "+simuQryListTmp.get(selQryIndex).simuPars.aggTime);
				simuCurTime+=simuQryListTmp.get(selQryIndex).simuPars.aggTime;	
				//System.out.println("size before updating agg time:"+bchSch.compStopTimeLst.size());
		                bchSch.compStopTimeLst.set(bchSch.compStopTimeLst.size()-1,simuCurTime);
		       		//System.out.println("size after updating agg time:"+bchSch.compStopTimeLst.size());
				//Logger.writeLog("Completed Agg for "+simuQryListTmp.get(selQryIndex).query_id+" @ "+simuCurTime);
				//totalComputeTime+=simuQryList.get(selQryIndex).simuPars.aggTime;
				//check if any query has failed 
				
				//simuQryListTmp.remove(selQryIndex);
		                simuQryListTmp.get(selQryIndex).completed=true;
		                //completionSts.add(selQryIndex,1);
		                //schPtValues.completionStsLst.add(schPtValues.simuTime.size()-1,completionSts);
		                //System.out.println("Setting compstoptime::"+(bchSch.compStopTimeLst.get(bchSch.compStopTimeLst.size()-1))+" "+(bchSch.compStopTimeLst.size()-1)+" "+(bchSch.curTime.size()-1));
		            }          
		        
		        /*Logger.writeLog("bchSchsizes:"+bchSch.batchRdyTimeLst.size()+" "+bchSch.compStartTimeLst.size()+" "+bchSch.compStopTimeLst.size()+
		                " "+bchSch.qryBatchNoLst.size()+" "+bchSch.qryIdLst.size()+" "+bchSch.reqNumNodesLst.size());*/
		    }		
				
		                      
		    //return obj;
		}

		public static void verifyConfig(ArrayList<Query> simuQryListTmp, float simuCurTime, MultiQryBchSch bchSch, QryBchSchPointValues schPtValues, int nodeIndex){
		   
		    bchSch.schGenerated = true;
		    float startSimuCurTime=simuCurTime;
		    /*float[] curSimuTime=new float[AWSCluster.curSessionWindEnd];
		    String[] selQryId=new String[AWSCluster.curSessionWindEnd];
		    float[] compStartTime=new float[AWSCluster.curSessionWindEnd];
		    float[] compStopTime=new float[AWSCluster.curSessionWindEnd];
		    int[] numNodes=new int[AWSCluster.curSessionWindEnd];    
		    float[] batchRdyTime=new float[AWSCluster.curSessionWindEnd];
		    int[] curBatchNo=new int[AWSCluster.curSessionWindEnd];
		    int index=0;*/
		    
		    for(int i=0;i<simuQryListTmp.size();i++) 
		            Logger.writeLog("checkPars:"+simuQryListTmp.get(i).query_id+" "+simuQryListTmp.get(i).simuPars.numTuplesPending+" "+simuQryListTmp.get(i).simuPars.curBatchReadyTime+" "+simuQryListTmp.get(i).simuPars.curBatchNo);
		    
		    boolean exitFlg=false;
		    while(!exitFlg) {
		        
		        /*ArrayList<Integer> numTuplesProcessed=new ArrayList<>();
		    	ArrayList<Integer> numTupleTotal=new ArrayList<>();
		    	ArrayList<Integer> numTuplesPending=new ArrayList<>();
		    	ArrayList<Integer> curBatchNo=new ArrayList<>();
		    	ArrayList<Integer> numBatchesProcessed=new ArrayList<>();
		    	ArrayList<Integer> curBatchSize=new ArrayList<>();
		    	ArrayList<Integer> numBatchesForAgg=new ArrayList<>();*/
		    
			//determine BRT & ST		
		        //check if nodes can be released
		        boolean resetConfig=false;

		        if(simuCurTime>=bchSch.maxTimeTBSched){            
		            for(int i=0;i<bchSch.curTime.size()-1;i++){
		                 //Logger.writeLog("checking for resetconfig:"+bchSch.curTime.get(i)+" "+bchSch.maxTimeTBSched+" "+bchSch.qryIdLst.get(i)+" "+bchSch.qryIdTBS+" "+bchSch.qryBatchNoLst.get(i)+" "+bchSch.qryBchNoTBS);
		                if(bchSch.curTime.get(i)>=bchSch.maxTimeTBSched && bchSch.qryIdLst.get(i)==bchSch.qryIdTBS && bchSch.qryBatchNoLst.get(i)==bchSch.qryBchNoTBS){
		                    resetConfig=true;
		                    break;
		                }
		            }
		        }
		       
			Logger.writeLog("CurTime::"+simuCurTime+" nodeIndex="+nodeIndex+" estNumNodesTmp="+Cluster.numNodes.get(nodeIndex));
			
		        //check if all queries completed
		        boolean allCompleted=true;
		        for(int i=0;i<simuQryListTmp.size();i++) {
		            if(simuQryListTmp.get(i).completed==false){
		                allCompleted=false;
		                break;
		            }
		        }
		        if(allCompleted){
		            bchSch.schGenerated=true;
		            exitFlg=true;
		            break;
		        }
		        ArrayList<String>  qryId=new ArrayList<String>();
		        ArrayList<Integer> numTuplesProcessed=new ArrayList<Integer>();
		        ArrayList<Integer> numTuplesTotal=new ArrayList<Integer>();
		        ArrayList<Integer> numTuplesPending=new ArrayList<Integer>();
		        ArrayList<Integer> curBatchNo=new ArrayList<Integer>();
		        ArrayList<Integer> numBatchesProcessed=new ArrayList<Integer>();
		        ArrayList<Integer> curBatchSize=new ArrayList<Integer>();
		        ArrayList<Integer> numBatchesForAgg=new ArrayList<Integer>();
		        ArrayList<Integer> completionSts=new ArrayList<Integer>();
		        
		        for(int i=0;i<simuQryListTmp.size();i++) {
		            numTuplesProcessed.add(simuQryListTmp.get(i).simuPars.numTuplesProcessed);
		            numTuplesTotal.add(simuQryListTmp.get(i).num_tuple_total);
		            numTuplesPending.add(simuQryListTmp.get(i).simuPars.numTuplesPending);
		            curBatchNo.add(simuQryListTmp.get(i).simuPars.curBatchNo);
		            numBatchesProcessed.add(simuQryListTmp.get(i).simuPars.numBatchesProcessed);
		            curBatchSize.add(simuQryListTmp.get(i).simuPars.curBatchSize);
		            numBatchesForAgg.add(simuQryListTmp.get(i).simuPars.numBatchesForAgg);
		            if(simuQryListTmp.get(i).completed==false)
		                completionSts.add(0);
		            else
		                completionSts.add(1);
		            qryId.add(simuQryListTmp.get(i).query_id);
		        }
			//System.out.println("estNumNodesTmp="+estNumNodesTmp);
			for(int i=0;i<simuQryListTmp.size();i++) { 
		            //System.out.println(simuQryListTmp.get(i).query_id+" "+simuQryListTmp.get(i).completed);
		            if(simuQryListTmp.get(i).completed==false){                
		            //Logger.writeLog("checkPars:"+simuCurTime+" "+simuQryListTmp.get(i).query_id+" "+simuQryListTmp.get(i).simuPars.numTuplesPending+" "+simuQryListTmp.get(i).simuPars.curBatchReadyTime);
		            if(simuCurTime>simuQryListTmp.get(i).wind_end_time) {
		              	simuQryListTmp.get(i).simuPars.curBatchReadyTime=simuQryListTmp.get(i).wind_end_time;
		                int tmpTuples=simuQryListTmp.get(i).EstTuples(nodeIndex,Cluster.cmax);
		                if(tmpTuples>simuQryListTmp.get(i).simuPars.numTuplesPending) {
		                    simuQryListTmp.get(i).simuPars.curBatchSize=simuQryListTmp.get(i).simuPars.numTuplesPending;
		                }
		                else {
		                    simuQryListTmp.get(i).simuPars.curBatchSize=tmpTuples;
		                }                                	
		                }                                    
		                else {
		                if(simuQryListTmp.get(i).simuPars.numTuplesPending<simuQryListTmp.get(i).simuPars.curBatchSize)
		                		simuQryListTmp.get(i).simuPars.curBatchSize=simuQryListTmp.get(i).simuPars.numTuplesPending;
		                 simuQryListTmp.get(i).simuPars.curBatchReadyTime=simuQryListTmp.get(i).ip_time_modified(simuQryListTmp.get(i).simuPars.numTuplesProcessed+
							simuQryListTmp.get(i).simuPars.curBatchSize,simuQryListTmp.get(i).input_rate);
		                }
		                }
		         }
		         
		            
		         if(resetConfig){
		         for(int i=0;i<simuQryListTmp.size();i++){
		         if(simuQryListTmp.get(i).completed==false){
		         	/*if(simuCurTime>simuQryListTmp.get(i).wind_end_time){
		         	    resetConfig=false;
		         	    break;
		         	}
		         	else if((simuQryListTmp.get(i).simuPars.curBatchReadyTime-simuCurTime)<(2*AWSCluster.clusterInitOH)){
		         	      resetConfig=false;
			              break;
		         	}*/
		                if(simuQryListTmp.get(i).simuPars.curBatchReadyTime<=simuCurTime){
		         	      resetConfig=false;
			              break;
		         	}
		         }
		         }
		         }
		         
		         if(resetConfig){
		            nodeIndex=Cluster.getNumNodeIndex(bchSch.startNumNodes);
		            Logger.writeLog("NumNodeIndex reset to::"+nodeIndex);
		         }
		            
		         for(int i=0;i<simuQryListTmp.size();i++){
		         if(simuQryListTmp.get(i).completed==false){
		         	                    
		                int numBatchesPending=(int)(simuQryListTmp.get(i).simuPars.numTuplesPending/simuQryListTmp.get(i).simuPars.curBatchSize);
				simuQryListTmp.get(i).simuPars.curBatchCompTime=simuQryListTmp.get(i).EstDuration(nodeIndex,simuQryListTmp.get(i).simuPars.curBatchSize);
				//Logger.writeLog("curBatchCompTime before and after::"+simuQryList.get(i).query_id+" "+simuQryList.get(i).simuPars.curBatchCompTime+" "+simuQryList.get(i).simuPars.numBatchesForAgg+" "+simuQryList.get(i).simuPars.curBatchNo+" "+simuQryList.get(i).simuPars.curBatchSize);
				
				simuQryListTmp.get(i).simuPars.curBatchAggTime=simuQryListTmp.get(i).compAggCost(nodeIndex,simuQryListTmp.get(i).simuPars.numBatchesForAgg);
						
				//Logger.writeLog("curBatchCompTime before and after::"+simuQryList.get(i).query_id+" "+simuQryList.get(i).simuPars.curBatchAggTime+" "+numBatchesPending);
				simuQryListTmp.get(i).simuPars.totalCompTime=numBatchesPending*simuQryListTmp.get(i).simuPars.curBatchCompTime;
				//Logger.writeLog("1:"+simuQryList.get(i).query_id+" "+simuQryList.get(i).simuPars.totalCompTime);
				if(simuQryListTmp.get(i).simuPars.numTuplesPending%simuQryListTmp.get(i).simuPars.curBatchSize>0) {	
					//Logger.writeLog("ForVerifyValues:"+simuQryList.get(i).query_id+" "+simuCurTime+" "+simuQryList.get(i).num_tuple_total+" numBatchesProcessed="+simuQryList.get(i).simuPars.numBatchesProcessed+
					//	" numBatchesPending="+numBatchesPending+" curBatchSize="+simuQryList.get(i).simuPars.curBatchSize+" numTuplesPending="+simuQryList.get(i).simuPars.numTuplesPending+" "+(simuQryList.get(i).simuPars.numTuplesPending%simuQryList.get(i).simuPars.curBatchSize));
					//VerifyValues(simuQryList.get(i).num_tuple_total,((simuQryList.get(i).simuPars.numBatchesProcessed+numBatchesPending-1)*simuQryList.get(i).simuPars.curBatchSize),8);
					simuQryListTmp.get(i).simuPars.totalCompTime+=simuQryListTmp.get(i).EstDuration(nodeIndex,(simuQryListTmp.get(i).simuPars.numTuplesPending%simuQryListTmp.get(i).simuPars.curBatchSize));							
					numBatchesPending++;		
					//Logger.writeLog("2:"+simuQryList.get(i).query_id+" "+simuQryList.get(i).simuPars.totalCompTime+" "+simuQryList.get(i).simuPars.numTuplesPending+" "+simuQryList.get(i).simuPars.curBatchSize);
				}
				
		                //System.out.println("totalCompTime-2 "+simuQryList.get(i).query_id+" "+simuQryList.get(i).simuPars.totalCompTime+" "+simuQryList.get(i).simuPars.curBatchAggTime+" batchesForAgg::"+simuQryList.get(i).simuPars.numBatchesForAgg+" "+numBatchesPending);
		                if(Cluster.numBatchPcntForParAgg!=1.0f)                                    
		                    simuQryListTmp.get(i).simuPars.totalCompTime+=(numBatchesPending/simuQryListTmp.get(i).simuPars.numBatchesForAgg)*simuQryListTmp.get(i).simuPars.curBatchAggTime;
		                            
		                    //System.out.println("check this::"+((numBatchesPending/simuQryList.get(i).simuPars.numBatchesForAgg)*simuQryList.get(i).simuPars.curBatchAggTime));    
		                if(Cluster.numBatchPcntForParAgg==1.0f)
		                    simuQryListTmp.get(i).simuPars.aggTime=simuQryListTmp.get(i).compAggCost(nodeIndex,((numBatchesPending+simuQryListTmp.get(i).simuPars.numBatchesProcessed-1)));
				else
		                    simuQryListTmp.get(i).simuPars.aggTime=simuQryListTmp.get(i).compAggCost(nodeIndex,((numBatchesPending+simuQryListTmp.get(i).simuPars.numBatchesProcessed-1)/simuQryListTmp.get(i).simuPars.numBatchesForAgg));	
						//simuQryList.get(i).simuPars.aggTime=simuQryList.get(i).compAggCost(nodeIndex,(numBatchesPending+simuQryList.get(i).simuPars.numBatchesProcessed-1));
				/*System.out.println(simuQryList.get(i).query_id+" Batch No:"+simuQryList.get(i).simuPars.numBatchesProcessed+" "+simuQryList.get(i).simuPars.curBatchReadyTime+
								" "+simuQryList.get(i).simuPars.curBatchCompTime+" "+simuQryList.get(i).simuPars.totalCompTime);*/
				simuQryListTmp.get(i).simuPars.totalCompTime+=simuQryListTmp.get(i).simuPars.aggTime;
				//System.out.println("totalCompTime-3 "+simuQryList.get(i).query_id+" "+simuQryList.get(i).simuPars.totalCompTime+" "+simuQryList.get(i).simuPars.aggTime);
				//Logger.writeLog("3:"+simuQryList.get(i).query_id+" "+simuQryList.get(i).simuPars.totalCompTime+" "+simuQryList.get(i).simuPars.aggTime+" "+simuQryList.get(i).simuPars.curBatchCompTime);
				//Logger.writeLog("curBatchCompTime before and after::"+simuQryList.get(i).query_id+" "+simuQryList.get(i).query_id+" "+simuQryList.get(i).simuPars.totalCompTime+" "+simuQryList.get(i).simuPars.aggTime+" "+simuQryList.get(i).simuPars.curBatchCompTime+" "+numBatchesPending+" "+simuQryList.get(i).simuPars.numBatchesProcessed);
				float compStartTimeTmp;
				if(simuCurTime>simuQryListTmp.get(i).simuPars.curBatchReadyTime)
		                    compStartTimeTmp=simuCurTime;
				else
		                    compStartTimeTmp=simuQryListTmp.get(i).simuPars.curBatchReadyTime;
						
						
						
				simuQryListTmp.get(i).simuPars.slackTime=simuQryListTmp.get(i).compSlackTimeGeneric(simuQryListTmp.get(i).deadline,compStartTimeTmp, simuQryListTmp.get(i).simuPars.totalCompTime);				
				Logger.writeLog(simuQryListTmp.get(i).query_id+" Batch No:"+simuQryListTmp.get(i).simuPars.numBatchesProcessed+" "+simuQryListTmp.get(i).simuPars.curBatchReadyTime+
								" "+simuQryListTmp.get(i).simuPars.curBatchCompTime+" "+simuQryListTmp.get(i).simuPars.totalCompTime+" "+
								simuQryListTmp.get(i).simuPars.numBatchesProcessed+" "+numBatchesPending+" curBchSize="+simuQryListTmp.get(i).simuPars.curBatchSize+" "+simuQryListTmp.get(i).simuPars.numBatchesForAgg+" "+simuQryListTmp.get(i).simuPars.aggTime);
				/*System.out.println(simuQryList.get(i).deadline+" "+
							simuQryList.get(i).simuPars.curBatchReadyTime+" "+simuQryList.get(i).simuPars.totalCompTime+" "+simuQryList.get(i).simuPars.curBatchSize);*/
				/*Logger.writeLog(simuQryListTmp.get(i).query_id+" slackTime="+simuQryListTmp.get(i).simuPars.slackTime+" compStartTime="+
								compStartTime+" totalCompTime="+simuQryListTmp.get(i).simuPars.totalCompTime+" curBchCompTime"+simuQryListTmp.get(i).simuPars.curBatchCompTime
								+" BRT:"+simuQryListTmp.get(i).simuPars.curBatchReadyTime+" deadline="+simuQryListTmp.get(i).deadline);*/
		            
		                numTuplesProcessed.set(i,simuQryListTmp.get(i).simuPars.numTuplesProcessed);
		                numTuplesTotal.set(i,simuQryListTmp.get(i).num_tuple_total);
		                numTuplesPending.set(i,simuQryListTmp.get(i).simuPars.numTuplesPending);
		                curBatchNo.set(i,simuQryListTmp.get(i).simuPars.curBatchNo);
		                numBatchesProcessed.set(i,simuQryListTmp.get(i).simuPars.numBatchesProcessed);
		                curBatchSize.set(i,simuQryListTmp.get(i).simuPars.curBatchSize);
		                numBatchesForAgg.set(i,simuQryListTmp.get(i).simuPars.numBatchesForAgg);
		                //completionSts.add(i,simuQryListTmp.get(i).completed);
		                //qryId.add(i,simuQryListTmp.get(i).query_id);
		            }
		            else{
		                //completionSts.add(i,true);
		                //qryId.add(i,simuQryListTmp.get(i).query_id);
		            }
		            }
					
		            //Adding to schPtValues
		            //schPtValues.simuTime.add(simuCurTime);
		            if(schPtValues.simuTime.size()==0)
		                schPtValues.simuTime.add(startSimuCurTime);
		            else
		                schPtValues.simuTime.add(bchSch.compStopTimeLst.get(bchSch.compStopTimeLst.size()-1));
		            schPtValues.numTuplesProcessedLst.add(numTuplesProcessed);
		            schPtValues.numTupleTotalLst.add(numTuplesTotal);
		            schPtValues.numTuplesPendingLst.add(numTuplesPending);
		            schPtValues.curBatchNoLst.add(curBatchNo);
		            schPtValues.numBatchesProcessedLst.add(numBatchesProcessed);
		            schPtValues.curBatchSizeLst.add(curBatchSize);
		            schPtValues.numBatchesForAggLst.add(numBatchesForAgg);
		            schPtValues.completionStsLst.add(completionSts);
		            schPtValues.qryIdLst.add(qryId);
		            
		            /*System.out.println("priting from SchPtValues::"+schPtValues.simuTime.get(schPtValues.simuTime.size()-1));
		            for(int kk=0;kk<simuQryListTmp.size();kk++)
		                System.out.println(schPtValues.qryIdLst.get(schPtValues.simuTime.size()-1).get(kk)+" "+schPtValues.numTuplesProcessedLst.get(schPtValues.simuTime.size()-1).get(kk)+" "+schPtValues.completionStsLst.get(schPtValues.simuTime.size()-1).get(kk)+" "+completionSts.get(kk));*/
		            
		            //sort queries
		            float minSlackTime=9999.0f;
		            float minBatchReadyTime=9999.0f;
		            int selQryIndex=-1;
					
		            
		            /*for(int i=0;i<simuQryListTmp.size();i++) {
				//System.out.println(simuQryList.get(i).query_id+" "+simuQryList.get(i).simuPars.curBatchReadyTime+" "+simuCurTime);
				if(simuQryListTmp.get(i).completed==false && simuQryListTmp.get(i).simuPars.slackTime<0) {                    
		                        minSlackTime=simuQryListTmp.get(i).simuPars.slackTime;
					selQryIndex=i;
		                        break;                   
				}
		            }*/
		            
		            if(selQryIndex==-1){
		            for(int i=0;i<simuQryListTmp.size();i++) {
				//System.out.println(simuQryList.get(i).query_id+" "+simuQryList.get(i).simuPars.curBatchReadyTime+" "+simuCurTime);
				if(simuQryListTmp.get(i).completed==false && simuQryListTmp.get(i).simuPars.curBatchReadyTime<=simuCurTime) {
		                    if(simuQryListTmp.get(i).simuPars.slackTime<minSlackTime) {
		                        minSlackTime=simuQryListTmp.get(i).simuPars.slackTime;
					selQryIndex=i;
		                    }
				}
		            }
		            }
		            
		            
			
		            
		            //System.out.println(selQryIndex+" "+minSlackTime);
		            if(selQryIndex!=-1) {
				//System.out.println("---------------------checkkkkkkkkkkkkkkkkkkkkkkkkk"+simuQryList.get(selQryIndex).query_id+" "+simuQryList.get(selQryIndex).simuPars.curBatchReadyTime+" "+simuCurTime);			
				//Logger.writeLog("selQry="+selQryIndex+" @ "+simuCurTime+" is "+simuQryListTmp.get(selQryIndex).query_id+" "+minSlackTime+" "+minBatchReadyTime);
		            }
						
		            if(selQryIndex==-1) {
		                minBatchReadyTime=9999.0f;
				minSlackTime=9999.0f;
				for(int i=0;i<simuQryListTmp.size();i++) {
				if(simuQryListTmp.get(i).completed==false && simuQryListTmp.get(i).simuPars.curBatchReadyTime<=minBatchReadyTime) {
		                    minBatchReadyTime=simuQryListTmp.get(i).simuPars.curBatchReadyTime;
				}
				}
				for(int i=0;i<simuQryListTmp.size();i++) {
		                    if(simuQryListTmp.get(i).completed==false && simuQryListTmp.get(i).simuPars.curBatchReadyTime==minBatchReadyTime) {				
					if(simuQryListTmp.get(i).simuPars.slackTime<minSlackTime) {
		                            minSlackTime=simuQryListTmp.get(i).simuPars.slackTime;
		                            selQryIndex=i;
					}
		                    }
				}
		            }
			
		            //System.out.println("selQry="+selQryIndex+" "+simuQryListTmp.get(selQryIndex).query_id);
		            //Logger.writeLog("selQry="+selQryIndex+" @ "+simuCurTime+" is "+simuQryList.get(selQryIndex).query_id+" "+minSlackTime+" "+minBatchReadyTime);
					
		       
		            //Adding to qryProcLstTmp
		            /*ArrayList<query> curSimuQList=new ArrayList<>();
		            for(int i=0;i<simuQryListTmp.size();i++)
		                curSimuQList.add(new query(simuQryListTmp.get(i)));*/
		           
		            
		        
		            if(simuQryListTmp.get(selQryIndex).simuPars.curBatchReadyTime>simuCurTime) {	
		            //Logger.writeLog("curBatchReadyTime="+simuQryList.get(selQryIndex).simuPars.curBatchReadyTime+" curBatchCompTime="+simuQryList.get(selQryIndex).simuPars.curBatchCompTime+" "+simuCurTime+" "+aggDur[nodeIndex]);
		            simuCurTime=simuQryListTmp.get(selQryIndex).simuPars.curBatchReadyTime+simuQryListTmp.get(selQryIndex).simuPars.curBatchCompTime;
		            if(Cluster.numBatchPcntForParAgg==1.0f && simuQryListTmp.get(selQryIndex).cur_batch_no>=simuQryListTmp.get(selQryIndex).simuPars.numBatchesForAgg)
		                simuCurTime+=simuQryListTmp.get(selQryIndex).simuPars.curBatchAggTime;
		            else if(Cluster.numBatchPcntForParAgg!=1.0f && simuQryListTmp.get(selQryIndex).cur_batch_no>=simuQryListTmp.get(selQryIndex).simuPars.numBatchesForAgg && (simuQryListTmp.get(selQryIndex).simuPars.curBatchNo%simuQryListTmp.get(selQryIndex).simuPars.numBatchesForAgg==0))
				simuCurTime+=simuQryListTmp.get(selQryIndex).simuPars.curBatchAggTime;
		            
		            //QryProcLstForSimuNew(float curSimuTime, String selQryId, float compStartTime, float compStopTime, int numNodes, float batchRdyTime, int curBatchNo,ArrayList<Query> curSimuQList)
		            
		            //bchSch.curTime.add(simuQryListTmp.get(selQryIndex).simuPars.curBatchReadyTime);
		            if(bchSch.curTime.size()==0)
		                bchSch.curTime.add(startSimuCurTime);
		            else
		                bchSch.curTime.add(bchSch.compStopTimeLst.get(bchSch.compStopTimeLst.size()-1));
		            bchSch.qryIdLst.add(simuQryListTmp.get(selQryIndex).query_id);
		            bchSch.compStartTimeLst.add(simuQryListTmp.get(selQryIndex).simuPars.curBatchReadyTime);
		            bchSch.compStopTimeLst.add(simuCurTime);
		            bchSch.reqNumNodesLst.add(Cluster.numNodes.get(nodeIndex));
			    bchSch.batchRdyTimeLst.add(simuQryListTmp.get(selQryIndex).simuPars.curBatchReadyTime);
		            bchSch.qryBatchNoLst.add(simuQryListTmp.get(selQryIndex).simuPars.curBatchNo);
		        
		      
		            
		            }
		            else{		
		            
		           
		            float startSimuTime=simuCurTime;
		            //Logger.writeLog("simuCurTime="+simuCurTime+" curBatchCompTime="+simuQryList.get(selQryIndex).simuPars.curBatchCompTime);
		            simuCurTime=simuCurTime+simuQryListTmp.get(selQryIndex).simuPars.curBatchCompTime;
		            if(Cluster.numBatchPcntForParAgg==1.0f && simuQryListTmp.get(selQryIndex).cur_batch_no>=simuQryListTmp.get(selQryIndex).simuPars.numBatchesForAgg)
		                simuCurTime+=simuQryListTmp.get(selQryIndex).simuPars.curBatchAggTime;
		            else if(Cluster.numBatchPcntForParAgg!=1.0f && simuQryListTmp.get(selQryIndex).cur_batch_no>=simuQryListTmp.get(selQryIndex).simuPars.numBatchesForAgg && (simuQryListTmp.get(selQryIndex).simuPars.curBatchNo%simuQryListTmp.get(selQryIndex).simuPars.numBatchesForAgg==0))
		        	simuCurTime+=simuQryListTmp.get(selQryIndex).simuPars.curBatchAggTime;
		            //Logger.writeLog("From qryProcLst: size="+qryProcLst.size()+" "+simuCurTime+" "+simuQryList.get(selQryIndex).query_id);
		            
		            //bchSch.curTime.add(startSimuTime);
		            if(bchSch.curTime.size()==0)
		                bchSch.curTime.add(startSimuCurTime);
		            else
		                bchSch.curTime.add(bchSch.compStopTimeLst.get(bchSch.compStopTimeLst.size()-1));
		            bchSch.qryIdLst.add(simuQryListTmp.get(selQryIndex).query_id);
		            bchSch.compStartTimeLst.add(startSimuTime);
		            bchSch.compStopTimeLst.add(simuCurTime);
		            bchSch.reqNumNodesLst.add(Cluster.numNodes.get(nodeIndex));
			    bchSch.batchRdyTimeLst.add(simuQryListTmp.get(selQryIndex).simuPars.curBatchReadyTime);
		            bchSch.qryBatchNoLst.add(simuQryListTmp.get(selQryIndex).simuPars.curBatchNo);      
		            
		            
		            }
		            //Logger.writeLog("qryProcLst.size()2=="+qryProcLst.size());
		            //totalComputeTime+=simuQryList.get(selQryIndex).simuPars.curBatchCompTime;
					
		            /*System.out.println(simuQryList.get(selQryIndex).query_id+" Batch:"+simuQryList.get(selQryIndex).simuPars.numBatchesProcessed
				+" processed @ "+simuCurTime+" with Config="+nodeIndex+" curBatchCompTime="+simuQryList.get(selQryIndex).simuPars.curBatchCompTime+" aggCost="+
				simuQryList.get(selQryIndex).simuPars.aggTime+" totalTuples="+simuQryList.get(selQryIndex).num_tuple_total);*/
		            //Logger.writeLog(simuQryList.get(selQryIndex).query_id+" Batch:"+simuQryList.get(selQryIndex).simuPars.numBatchesProcessed+" processed @ "+simuCurTime+" with Config="+nodeIndex+" curBatchCompTime="+simuQryList.get(selQryIndex).simuPars.curBatchCompTime+" aggCost="+
		            //	simuQryList.get(selQryIndex).simuPars.aggTime+" totalTuples="+simuQryList.get(selQryIndex).num_tuple_total+" processed="+simuQryList.get(selQryIndex).simuPars.numTuplesProcessed+" curbchsize="+simuQryList.get(selQryIndex).simuPars.curBatchSize);
		            Logger.writeLog("Processssed "+simuQryListTmp.get(selQryIndex).query_id+" "+simuQryListTmp.get(selQryIndex).simuPars.curBatchReadyTime+" "+simuQryListTmp.get(selQryIndex).simuPars.curBatchCompTime+" "+simuCurTime+" "+simuQryListTmp.get(selQryIndex).simuPars.numTuplesPending+" "+simuQryListTmp.get(selQryIndex).simuPars.numTuplesProcessed+" "+simuQryListTmp.get(selQryIndex).simuPars.curBatchSize+" "+Cluster.numNodes.get(nodeIndex));
		            Logger.writeLog("bchSch size="+bchSch.curTime.size()+" schPtvalues size="+schPtValues.simuTime.size());
		            simuQryListTmp.get(selQryIndex).simuPars.curNumNodes=Cluster.numNodes.get(nodeIndex);
		            simuQryListTmp.get(selQryIndex).simuPars.numBatchesProcessed++;
		            simuQryListTmp.get(selQryIndex).simuPars.curBatchNo++;
		            simuQryListTmp.get(selQryIndex).simuPars.numTuplesProcessed+=simuQryListTmp.get(selQryIndex).simuPars.curBatchSize;							
		            if(simuQryListTmp.get(selQryIndex).simuPars.numTuplesPending<simuQryListTmp.get(selQryIndex).simuPars.curBatchSize) {
		            	simuQryListTmp.get(selQryIndex).simuPars.curBatchSize=simuQryListTmp.get(selQryIndex).simuPars.numTuplesPending;
		            	simuQryListTmp.get(selQryIndex).simuPars.numTuplesPending=0;
		            }
		            else {
		        
		            	simuQryListTmp.get(selQryIndex).simuPars.numTuplesPending-=simuQryListTmp.get(selQryIndex).simuPars.curBatchSize;
		            }
		            //System.out.println(simuQryListTmp.get(selQryIndex).query_id+" numTuplesPending=="+simuQryListTmp.get(selQryIndex).simuPars.numTuplesPending+" "+simuQryListTmp.get(selQryIndex).num_tuple_total);
		            //Logger.writeLog("simuCurTime=="+simuCurTime);
		            
		            
		            if(simuQryListTmp.get(selQryIndex).simuPars.slackTime<0){
		            Logger.writeLog("Negative Slack Time for "+simuQryListTmp.get(selQryIndex).query_id+" @ "+simuCurTime+" with RdyTime="+simuQryListTmp.get(selQryIndex).simuPars.curBatchReadyTime+" ST:"+simuQryListTmp.get(selQryIndex).simuPars.slackTime);
		            bchSch.schGenerated=false;
		            exitFlg=true;
		            break;
		            }
		            
		            if(simuQryListTmp.get(selQryIndex).simuPars.numTuplesProcessed>=simuQryListTmp.get(selQryIndex).num_tuple_total) {
		                Logger.writeLog("doing final agg for "+simuQryListTmp.get(selQryIndex).query_id+" @ "+simuCurTime+" "+simuQryListTmp.get(selQryIndex).simuPars.aggTime);
				simuCurTime+=simuQryListTmp.get(selQryIndex).simuPars.aggTime;	
				//System.out.println("size before updating agg time:"+bchSch.compStopTimeLst.size());
		                bchSch.compStopTimeLst.set(bchSch.compStopTimeLst.size()-1,simuCurTime);
		       		//System.out.println("size after updating agg time:"+bchSch.compStopTimeLst.size());
				//Logger.writeLog("Completed Agg for "+simuQryListTmp.get(selQryIndex).query_id+" @ "+simuCurTime);
				//totalComputeTime+=simuQryList.get(selQryIndex).simuPars.aggTime;
				//check if any query has failed 
				if((int)simuCurTime>(int)simuQryListTmp.get(selQryIndex).deadline) {
		                    Logger.writeLog("missed deadline deadline="+simuQryListTmp.get(selQryIndex).deadline+" curSimuTime="+simuCurTime+" "+simuQryListTmp.get(selQryIndex).simuPars.numTuplesProcessed+" "+simuQryListTmp.get(selQryIndex).simuPars.numTuplesPending);
		                    bchSch.schGenerated=false;
		                    exitFlg=true;
		                    break;                    
				}
				//simuQryListTmp.remove(selQryIndex);
		                simuQryListTmp.get(selQryIndex).completed=true;
		                //completionSts.add(selQryIndex,1);
		                //schPtValues.completionStsLst.add(schPtValues.simuTime.size()-1,completionSts);
		                //System.out.println("Setting compstoptime::"+(bchSch.compStopTimeLst.get(bchSch.compStopTimeLst.size()-1))+" "+(bchSch.compStopTimeLst.size()-1)+" "+(bchSch.curTime.size()-1));
		            }          
		        
		        /*Logger.writeLog("bchSchsizes:"+bchSch.batchRdyTimeLst.size()+" "+bchSch.compStartTimeLst.size()+" "+bchSch.compStopTimeLst.size()+
		                " "+bchSch.qryBatchNoLst.size()+" "+bchSch.qryIdLst.size()+" "+bchSch.reqNumNodesLst.size());*/
		    }		
				
		                      
		    //return obj;
		}
		//public MultiQrySch runSimulation(float rslFact,int nodeIndex,float modiIpRate, boolean addNodes) {


public static void genOnlyLLFSchMain(float cur_time,List<Query> q_list){
		    
		     
		   for(int c=0;c<5;c++) {
			   int  nodeIndex=c;
			   int startNodeIndex=nodeIndex;	 
			   float simuCurTime=cur_time;
			   float simuStartTime=cur_time;
			   //float totalComputeTime=0.0f;
			   ArrayList<Query> simuQryList=new ArrayList<Query>();
			   ArrayList<Query> simuQryList2=new ArrayList<Query>();
			   ArrayList<Query> simuQryListTmp=new ArrayList<Query>();   
			   MultiQryBchSch bchSch = new MultiQryBchSch();
		   
		        
		     
			   simuQryList.clear();
			   simuQryList2.clear();
			   simuQryListTmp.clear();
		            
		    float minWindStartTime=9999.0f;
		    for(int i=0;i<q_list.size();i++) {			
			if(q_list.get(i).completed==false){
		            if(q_list.get(i).wind_start_time<minWindStartTime) {
				minWindStartTime=q_list.get(i).wind_start_time;
		                Logger.writeLog("simuCurTime updated to "+simuCurTime+" from "+simuCurTime);
		            }
		        }
		    }
		    if(simuCurTime<minWindStartTime)
		        simuCurTime=minWindStartTime;
		    //Logger.writeLog("simuCurTime updated to "+simuCurTime);
		    
		    bchSch.startTime=simuCurTime;
		    
		   
		   
		        
			for(int i=0;i<q_list.size();i++) {	
		           
		            if(q_list.get(i).completed==false){	
		                Query tmpQry=new Query(q_list.get(i));
				//System.out.println(this.q_list.get(i).query_id+" "+q_list.get(i).num_tuple_total);				
				tmpQry.simuPars.numTuplesProcessed=q_list.get(i).num_tuple_processed;
		             
				int extraTuples=0;
				if(simuCurTime<q_list.get(i).wind_end_time && simuCurTime>=q_list.get(i).wind_start_time) {				
		                    extraTuples=(int)((q_list.get(i).wind_end_time-simuCurTime)*(q_list.get(i).input_rate));
		                    tmpQry.num_tuple_total=(int)(tmpQry.compFileNoWithIpRate(simuCurTime)+extraTuples-tmpQry.compFileNoWithIpRate(tmpQry.wind_start_time-1));
		                   
				}
				else if(simuCurTime<q_list.get(i).wind_end_time && simuCurTime<q_list.get(i).wind_start_time) {				
		                    //tmpQry.num_tuple_total=(int)(tmpQry.compFileNoWithIpRate(tmpQry.wind_end_time)-tmpQry.compFileNoWithIpRate(tmpQry.wind_start_time-1));
		                    //Logger.writeLog(tmpQry.compFileNoWithIpRate(tmpQry.wind_end_time)+" "+tmpQry.compFileNoWithIpRate(tmpQry.wind_start_time-1)+" 22 ");
		                    tmpQry.num_tuple_total=(int)((q_list.get(i).wind_end_time-q_list.get(i).wind_start_time+1)*q_list.get(i).input_rate);
		                   
				}
		        else{
		                  tmpQry.num_tuple_total=(int)(tmpQry.compFileNoWithIpRate(simuCurTime)-tmpQry.compFileNoWithIpRate(tmpQry.wind_start_time-1));
		                   // Logger.writeLog(tmpQry.compFileNoWithIpRate(simuCurTime)+" "+tmpQry.compFileNoWithIpRate(tmpQry.wind_start_time-1)+" 33 ");
		                 
		       }
		                                
							
				//System.out.println(cur_time+" "+tmpQry.compFileNoWithIpRate(simuCurTime)+" "+extraTuples+" "+tmpQry.num_tuple_total+"  for checkingggg");
				
				tmpQry.simuPars.numTuplesPending=tmpQry.num_tuple_total-tmpQry.simuPars.numTuplesProcessed;
				tmpQry.simuPars.curBatchNo=q_list.get(i).cur_batch_no+1;
				tmpQry.simuPars.numBatchesProcessed=q_list.get(i).cur_batch_no;				
				//System.out.println(simuQryList.get(i).query_id+" "+simuQryList.get(i).simuPars.numTuplesPending);
		              
		                
				simuQryList.add(tmpQry);
		        simuQryList2.add(tmpQry);  
		        simuQryListTmp.add(tmpQry);
				
		        }
			}		
			
		       
		        //System.out.println(bchSch.curIndex+" "+schPtValues.simuTime.size());
			if(simuCurTime<minWindStartTime)
		            simuCurTime=minWindStartTime;
		        
		        
		        //System.out.println("simuCurTime is::"+simuCurTime);
		       /* simuQryListTmp.clear();
		        for(int i=0;i<this.q_list.size();i++) {			
		            if(this.q_list.get(i).completed==false){	
		                query tmpQry=new query(this.q_list.get(i));
		                simuQryListTmp.add(tmpQry);
		            }
		        }*/
		            
		      
		     //minconfig run
		        genOnlyLLFSch(simuQryListTmp, simuCurTime,  bchSch, nodeIndex);
		        Logger.writeLog("ToLog::************************");
		        Logger.writeLog("ToLog::Printing Last batches of Queries scheduled with "+Cluster.numNodes.get(nodeIndex));
			Logger.writeLog("ToLog::Index QryID  BST   BET  Deadline NumNodes Result");
		        /*for(int i=0;i<bchSch.curTime.size();i++){
		            Logger.writeLog("ToLog::"+i+" "+bchSch.qryIdLst.get(i)+" "+bchSch.compStartTimeLst.get(i)+" "+bchSch.compStopTimeLst.get(i)+" "+bchSch.reqNumNodesLst.get(i));
		            //System.out.println(i+" "+bchSch.qryIdLst.get(i)+" "+bchSch.compStartTimeLst.get(i)+" "+bchSch.compStopTimeLst.get(i)+" "+bchSch.reqNumNodesLst.get(i));
		        }*/
                        writeResToFile(bchSch,q_list);
		        Logger.writeLog("ToLog::************************");
		        CompCostFrmNodeSch(bchSch,bchSch.compStopTimeLst.get(bchSch.compStopTimeLst.size()-1));
		   }
		       
	}
		
	public static void genOnlyLLFSch(ArrayList<Query> simuQryListTmp, float simuCurTime, MultiQryBchSch bchSch, int nodeIndex){
		   
		    bchSch.schGenerated = true;
		    float startSimuCurTime=simuCurTime;
		  
		    
		    for(int i=0;i<simuQryListTmp.size();i++) 
		            Logger.writeLog("checkPars:"+simuQryListTmp.get(i).query_id+" "+simuQryListTmp.get(i).simuPars.numTuplesPending+" "+simuQryListTmp.get(i).simuPars.curBatchReadyTime+" "+simuQryListTmp.get(i).simuPars.curBatchNo);
		    
		    boolean exitFlg=false;
		    while(!exitFlg) {
		        
		      
		    
			
		       
			Logger.writeLog("CurTime::"+simuCurTime+" nodeIndex="+nodeIndex+" estNumNodesTmp="+Cluster.numNodes.get(nodeIndex));
			
		        //check if all queries completed
		        boolean allCompleted=true;
		        for(int i=0;i<simuQryListTmp.size();i++) {
		            if(simuQryListTmp.get(i).completed==false){
		                allCompleted=false;
		                break;
		            }
		        }
		        if(allCompleted){
		            bchSch.schGenerated=true;
		            exitFlg=true;
		            break;
		        }
		       
		        
		      
			//System.out.println("estNumNodesTmp="+estNumNodesTmp);
			for(int i=0;i<simuQryListTmp.size();i++) { 
		            //System.out.println(simuQryListTmp.get(i).query_id+" "+simuQryListTmp.get(i).completed);
		            if(simuQryListTmp.get(i).completed==false){                
		            //Logger.writeLog("checkPars:"+simuCurTime+" "+simuQryListTmp.get(i).query_id+" "+simuQryListTmp.get(i).simuPars.numTuplesPending+" "+simuQryListTmp.get(i).simuPars.curBatchReadyTime);
		            if(simuCurTime>simuQryListTmp.get(i).wind_end_time) {
		              	simuQryListTmp.get(i).simuPars.curBatchReadyTime=simuQryListTmp.get(i).wind_end_time;
		                /*int tmpTuples=simuQryListTmp.get(i).EstTuples(nodeIndex,AWSCluster.cmax);
		                if(tmpTuples>simuQryListTmp.get(i).simuPars.numTuplesPending) {
		                    simuQryListTmp.get(i).simuPars.curBatchSize=simuQryListTmp.get(i).simuPars.numTuplesPending;
		                }
		                else {
		                    simuQryListTmp.get(i).simuPars.curBatchSize=tmpTuples;
		                } */                               	
		                }                                    
		                else {
		                /*if(simuQryListTmp.get(i).simuPars.numTuplesPending<simuQryListTmp.get(i).simuPars.curBatchSize)
		                		simuQryListTmp.get(i).simuPars.curBatchSize=simuQryListTmp.get(i).simuPars.numTuplesPending;
		                 simuQryListTmp.get(i).simuPars.curBatchReadyTime=simuQryListTmp.get(i).ip_time_modified(simuQryListTmp.get(i).simuPars.numTuplesProcessed+
							simuQryListTmp.get(i).simuPars.curBatchSize,simuQryListTmp.get(i).input_rate);*/
		                 simuQryListTmp.get(i).simuPars.curBatchReadyTime=simuCurTime;
		                }
		                }
		         }
		         
		            
		        
		            
		         for(int i=0;i<simuQryListTmp.size();i++){
		         if(simuQryListTmp.get(i).completed==false){
		         	//System.out.println("nodeindexxxx is "+nodeIndex+" @ "+simuCurTime);                    
		                simuQryListTmp.get(i).simuPars.curBatchSize=simuQryListTmp.get(i).compFileNoWithIpRate(simuCurTime)-simuQryListTmp.get(i).simuPars.numTuplesProcessed;
		                int numBatchesPending=(int)(simuQryListTmp.get(i).simuPars.numTuplesPending/simuQryListTmp.get(i).simuPars.curBatchSize);
				simuQryListTmp.get(i).simuPars.curBatchCompTime=simuQryListTmp.get(i).EstDuration(nodeIndex,simuQryListTmp.get(i).simuPars.curBatchSize);
				//Logger.writeLog("curBatchCompTime before and after::"+simuQryList.get(i).query_id+" "+simuQryList.get(i).simuPars.curBatchCompTime+" "+simuQryList.get(i).simuPars.numBatchesForAgg+" "+simuQryList.get(i).simuPars.curBatchNo+" "+simuQryList.get(i).simuPars.curBatchSize);
				
				//simuQryListTmp.get(i).simuPars.curBatchAggTime=simuQryListTmp.get(i).compAggCost(nodeIndex,simuQryListTmp.get(i).simuPars.numBatchesForAgg);
						
				//Logger.writeLog("curBatchCompTime before and after::"+simuQryList.get(i).query_id+" "+simuQryList.get(i).simuPars.curBatchAggTime+" "+numBatchesPending);
				simuQryListTmp.get(i).simuPars.totalCompTime=numBatchesPending*simuQryListTmp.get(i).simuPars.curBatchCompTime;
				//Logger.writeLog("1:"+simuQryList.get(i).query_id+" "+simuQryList.get(i).simuPars.totalCompTime);
				if(simuQryListTmp.get(i).simuPars.numTuplesPending%simuQryListTmp.get(i).simuPars.curBatchSize>0) {	
					//Logger.writeLog("ForVerifyValues:"+simuQryList.get(i).query_id+" "+simuCurTime+" "+simuQryList.get(i).num_tuple_total+" numBatchesProcessed="+simuQryList.get(i).simuPars.numBatchesProcessed+
					//	" numBatchesPending="+numBatchesPending+" curBatchSize="+simuQryList.get(i).simuPars.curBatchSize+" numTuplesPending="+simuQryList.get(i).simuPars.numTuplesPending+" "+(simuQryList.get(i).simuPars.numTuplesPending%simuQryList.get(i).simuPars.curBatchSize));
					//VerifyValues(simuQryList.get(i).num_tuple_total,((simuQryList.get(i).simuPars.numBatchesProcessed+numBatchesPending-1)*simuQryList.get(i).simuPars.curBatchSize),8);
					simuQryListTmp.get(i).simuPars.totalCompTime+=simuQryListTmp.get(i).EstDuration(nodeIndex,(simuQryListTmp.get(i).simuPars.numTuplesPending%simuQryListTmp.get(i).simuPars.curBatchSize));							
					numBatchesPending++;		
					//Logger.writeLog("2:"+simuQryList.get(i).query_id+" "+simuQryList.get(i).simuPars.totalCompTime+" "+simuQryList.get(i).simuPars.numTuplesPending+" "+simuQryList.get(i).simuPars.curBatchSize);
				}
				
		                //System.out.println("totalCompTime-2 "+simuQryList.get(i).query_id+" "+simuQryList.get(i).simuPars.totalCompTime+" "+simuQryList.get(i).simuPars.curBatchAggTime+" batchesForAgg::"+simuQryList.get(i).simuPars.numBatchesForAgg+" "+numBatchesPending);
		                if(Cluster.numBatchPcntForParAgg!=1.0f)                                    
		                    simuQryListTmp.get(i).simuPars.totalCompTime+=(numBatchesPending/simuQryListTmp.get(i).simuPars.numBatchesForAgg)*simuQryListTmp.get(i).simuPars.curBatchAggTime;
		                            
		                    //System.out.println("check this::"+((numBatchesPending/simuQryList.get(i).simuPars.numBatchesForAgg)*simuQryList.get(i).simuPars.curBatchAggTime));    
		                if(Cluster.numBatchPcntForParAgg==1.0f)
		                    simuQryListTmp.get(i).simuPars.aggTime=simuQryListTmp.get(i).compAggCost(nodeIndex,((numBatchesPending+simuQryListTmp.get(i).simuPars.numBatchesProcessed-1)));
				else
		                    simuQryListTmp.get(i).simuPars.aggTime=simuQryListTmp.get(i).compAggCost(nodeIndex,((numBatchesPending+simuQryListTmp.get(i).simuPars.numBatchesProcessed-1)/simuQryListTmp.get(i).simuPars.numBatchesForAgg));	
						//simuQryList.get(i).simuPars.aggTime=simuQryList.get(i).compAggCost(nodeIndex,(numBatchesPending+simuQryList.get(i).simuPars.numBatchesProcessed-1));
				/*System.out.println(simuQryList.get(i).query_id+" Batch No:"+simuQryList.get(i).simuPars.numBatchesProcessed+" "+simuQryList.get(i).simuPars.curBatchReadyTime+
								" "+simuQryList.get(i).simuPars.curBatchCompTime+" "+simuQryList.get(i).simuPars.totalCompTime);*/
				simuQryListTmp.get(i).simuPars.totalCompTime+=simuQryListTmp.get(i).simuPars.aggTime;
				//System.out.println("totalCompTime-3 "+simuQryList.get(i).query_id+" "+simuQryList.get(i).simuPars.totalCompTime+" "+simuQryList.get(i).simuPars.aggTime);
				//Logger.writeLog("3:"+simuQryList.get(i).query_id+" "+simuQryList.get(i).simuPars.totalCompTime+" "+simuQryList.get(i).simuPars.aggTime+" "+simuQryList.get(i).simuPars.curBatchCompTime);
				//Logger.writeLog("curBatchCompTime before and after::"+simuQryList.get(i).query_id+" "+simuQryList.get(i).query_id+" "+simuQryList.get(i).simuPars.totalCompTime+" "+simuQryList.get(i).simuPars.aggTime+" "+simuQryList.get(i).simuPars.curBatchCompTime+" "+numBatchesPending+" "+simuQryList.get(i).simuPars.numBatchesProcessed);
				float compStartTimeTmp;
				if(simuCurTime>simuQryListTmp.get(i).simuPars.curBatchReadyTime)
		                    compStartTimeTmp=simuCurTime;
				else
		                    compStartTimeTmp=simuQryListTmp.get(i).simuPars.curBatchReadyTime;
						
						
						
				simuQryListTmp.get(i).simuPars.slackTime=simuQryListTmp.get(i).compSlackTimeGeneric(simuQryListTmp.get(i).deadline,compStartTimeTmp, simuQryListTmp.get(i).simuPars.totalCompTime);				
				Logger.writeLog(simuQryListTmp.get(i).query_id+" Batch No:"+simuQryListTmp.get(i).simuPars.numBatchesProcessed+" "+simuQryListTmp.get(i).simuPars.curBatchReadyTime+
								" "+simuQryListTmp.get(i).simuPars.curBatchCompTime+" "+simuQryListTmp.get(i).simuPars.totalCompTime+" "+
								simuQryListTmp.get(i).simuPars.numBatchesProcessed+" "+numBatchesPending+" curBchSize="+simuQryListTmp.get(i).simuPars.curBatchSize+" "+simuQryListTmp.get(i).simuPars.numBatchesForAgg+" "+simuQryListTmp.get(i).simuPars.aggTime);
				/*System.out.println(simuQryList.get(i).deadline+" "+
							simuQryList.get(i).simuPars.curBatchReadyTime+" "+simuQryList.get(i).simuPars.totalCompTime+" "+simuQryList.get(i).simuPars.curBatchSize);*/
				/*Logger.writeLog(simuQryListTmp.get(i).query_id+" slackTime="+simuQryListTmp.get(i).simuPars.slackTime+" compStartTime="+
								compStartTime+" totalCompTime="+simuQryListTmp.get(i).simuPars.totalCompTime+" curBchCompTime"+simuQryListTmp.get(i).simuPars.curBatchCompTime
								+" BRT:"+simuQryListTmp.get(i).simuPars.curBatchReadyTime+" deadline="+simuQryListTmp.get(i).deadline);*/
		            
		             
		                //completionSts.add(i,simuQryListTmp.get(i).completed);
		                //qryId.add(i,simuQryListTmp.get(i).query_id);
		            }
		            else{
		                //completionSts.add(i,true);
		                //qryId.add(i,simuQryListTmp.get(i).query_id);
		            }
		            }
					
		           
		            
		            /*System.out.println("priting from SchPtValues::"+schPtValues.simuTime.get(schPtValues.simuTime.size()-1));
		            for(int kk=0;kk<simuQryListTmp.size();kk++)
		                System.out.println(schPtValues.qryIdLst.get(schPtValues.simuTime.size()-1).get(kk)+" "+schPtValues.numTuplesProcessedLst.get(schPtValues.simuTime.size()-1).get(kk)+" "+schPtValues.completionStsLst.get(schPtValues.simuTime.size()-1).get(kk)+" "+completionSts.get(kk));*/
		            
		            //sort queries
		            float minSlackTime=9999.0f;
		            float minBatchReadyTime=9999.0f;
		            int selQryIndex=-1;
					
		            
		            /*for(int i=0;i<simuQryListTmp.size();i++) {
				//System.out.println(simuQryList.get(i).query_id+" "+simuQryList.get(i).simuPars.curBatchReadyTime+" "+simuCurTime);
				if(simuQryListTmp.get(i).completed==false && simuQryListTmp.get(i).simuPars.slackTime<0) {                    
		                        minSlackTime=simuQryListTmp.get(i).simuPars.slackTime;
					selQryIndex=i;
		                        break;                   
				}
		            }*/
		            
		            if(selQryIndex==-1){
		            for(int i=0;i<simuQryListTmp.size();i++) {
				//System.out.println(simuQryList.get(i).query_id+" "+simuQryList.get(i).simuPars.curBatchReadyTime+" "+simuCurTime);
				if(simuQryListTmp.get(i).completed==false && simuQryListTmp.get(i).simuPars.curBatchReadyTime<=simuCurTime) {
		                    if(simuQryListTmp.get(i).simuPars.slackTime<minSlackTime) {
		                        minSlackTime=simuQryListTmp.get(i).simuPars.slackTime;
					selQryIndex=i;
		                    }
				}
		            }
		            }
		            
		            
			
		           
		            //System.out.println(selQryIndex+" "+minSlackTime);
		            if(selQryIndex!=-1) {
				//System.out.println("---------------------checkkkkkkkkkkkkkkkkkkkkkkkkk"+simuQryList.get(selQryIndex).query_id+" "+simuQryList.get(selQryIndex).simuPars.curBatchReadyTime+" "+simuCurTime);			
				//Logger.writeLog("selQry="+selQryIndex+" @ "+simuCurTime+" is "+simuQryListTmp.get(selQryIndex).query_id+" "+minSlackTime+" "+minBatchReadyTime);
		            }
						
		            if(selQryIndex==-1) {
		                minBatchReadyTime=9999.0f;
				minSlackTime=9999.0f;
				for(int i=0;i<simuQryListTmp.size();i++) {
				if(simuQryListTmp.get(i).completed==false && simuQryListTmp.get(i).simuPars.curBatchReadyTime<=minBatchReadyTime) {
		                    minBatchReadyTime=simuQryListTmp.get(i).simuPars.curBatchReadyTime;
				}
				}
				for(int i=0;i<simuQryListTmp.size();i++) {
		                    if(simuQryListTmp.get(i).completed==false && simuQryListTmp.get(i).simuPars.curBatchReadyTime==minBatchReadyTime) {				
					if(simuQryListTmp.get(i).simuPars.slackTime<minSlackTime) {
		                            minSlackTime=simuQryListTmp.get(i).simuPars.slackTime;
		                            selQryIndex=i;
					}
		                    }
				}
		            }
			
		            //System.out.println("selQry="+selQryIndex+" "+simuQryListTmp.get(selQryIndex).query_id);
		            //Logger.writeLog("selQry="+selQryIndex+" @ "+simuCurTime+" is "+simuQryList.get(selQryIndex).query_id+" "+minSlackTime+" "+minBatchReadyTime);
					
		       
		            //Adding to qryProcLstTmp
		            /*ArrayList<query> curSimuQList=new ArrayList<>();
		            for(int i=0;i<simuQryListTmp.size();i++)
		                curSimuQList.add(new query(simuQryListTmp.get(i)));*/
		           
		            
		        
		            if(simuQryListTmp.get(selQryIndex).simuPars.curBatchReadyTime>simuCurTime) {	
		            //Logger.writeLog("curBatchReadyTime="+simuQryList.get(selQryIndex).simuPars.curBatchReadyTime+" curBatchCompTime="+simuQryList.get(selQryIndex).simuPars.curBatchCompTime+" "+simuCurTime+" "+aggDur[nodeIndex]);
		            simuCurTime=simuQryListTmp.get(selQryIndex).simuPars.curBatchReadyTime+simuQryListTmp.get(selQryIndex).simuPars.curBatchCompTime;
		            if(Cluster.numBatchPcntForParAgg==1.0f && simuQryListTmp.get(selQryIndex).cur_batch_no>=simuQryListTmp.get(selQryIndex).simuPars.numBatchesForAgg)
		                simuCurTime+=simuQryListTmp.get(selQryIndex).simuPars.curBatchAggTime;
		            else if(Cluster.numBatchPcntForParAgg!=1.0f && simuQryListTmp.get(selQryIndex).cur_batch_no>=simuQryListTmp.get(selQryIndex).simuPars.numBatchesForAgg && (simuQryListTmp.get(selQryIndex).simuPars.curBatchNo%simuQryListTmp.get(selQryIndex).simuPars.numBatchesForAgg==0))
				simuCurTime+=simuQryListTmp.get(selQryIndex).simuPars.curBatchAggTime;
		            
		            //QryProcLstForSimuNew(float curSimuTime, String selQryId, float compStartTime, float compStopTime, int numNodes, float batchRdyTime, int curBatchNo,ArrayList<Query> curSimuQList)
		            
		            //bchSch.curTime.add(simuQryListTmp.get(selQryIndex).simuPars.curBatchReadyTime);
		            if(bchSch.curTime.size()==0)
		                bchSch.curTime.add(startSimuCurTime);
		            else
		                bchSch.curTime.add(bchSch.compStopTimeLst.get(bchSch.compStopTimeLst.size()-1));
		            bchSch.qryIdLst.add(simuQryListTmp.get(selQryIndex).query_id);
		            bchSch.compStartTimeLst.add(simuQryListTmp.get(selQryIndex).simuPars.curBatchReadyTime);
		            bchSch.compStopTimeLst.add(simuCurTime);
		            bchSch.reqNumNodesLst.add(Cluster.numNodes.get(nodeIndex));
			    bchSch.batchRdyTimeLst.add(simuQryListTmp.get(selQryIndex).simuPars.curBatchReadyTime);
		            bchSch.qryBatchNoLst.add(simuQryListTmp.get(selQryIndex).simuPars.curBatchNo);
		        
		      
		            
		            }
		            else{		
		            
		           
		            float startSimuTime=simuCurTime;
		            //Logger.writeLog("simuCurTime="+simuCurTime+" curBatchCompTime="+simuQryList.get(selQryIndex).simuPars.curBatchCompTime);
		            simuCurTime=simuCurTime+simuQryListTmp.get(selQryIndex).simuPars.curBatchCompTime;
		            if(Cluster.numBatchPcntForParAgg==1.0f && simuQryListTmp.get(selQryIndex).cur_batch_no>=simuQryListTmp.get(selQryIndex).simuPars.numBatchesForAgg)
		                simuCurTime+=simuQryListTmp.get(selQryIndex).simuPars.curBatchAggTime;
		            else if(Cluster.numBatchPcntForParAgg!=1.0f && simuQryListTmp.get(selQryIndex).cur_batch_no>=simuQryListTmp.get(selQryIndex).simuPars.numBatchesForAgg && (simuQryListTmp.get(selQryIndex).simuPars.curBatchNo%simuQryListTmp.get(selQryIndex).simuPars.numBatchesForAgg==0))
		        	simuCurTime+=simuQryListTmp.get(selQryIndex).simuPars.curBatchAggTime;
		            //Logger.writeLog("From qryProcLst: size="+qryProcLst.size()+" "+simuCurTime+" "+simuQryList.get(selQryIndex).query_id);
		            
		            //bchSch.curTime.add(startSimuTime);
		            if(bchSch.curTime.size()==0)
		                bchSch.curTime.add(startSimuCurTime);
		            else
		                bchSch.curTime.add(bchSch.compStopTimeLst.get(bchSch.compStopTimeLst.size()-1));
		            bchSch.qryIdLst.add(simuQryListTmp.get(selQryIndex).query_id);
		            bchSch.compStartTimeLst.add(startSimuTime);
		            bchSch.compStopTimeLst.add(simuCurTime);
		            bchSch.reqNumNodesLst.add(Cluster.numNodes.get(nodeIndex));
			    bchSch.batchRdyTimeLst.add(simuQryListTmp.get(selQryIndex).simuPars.curBatchReadyTime);
		            bchSch.qryBatchNoLst.add(simuQryListTmp.get(selQryIndex).simuPars.curBatchNo);      
		            
		            
		            }
		            //Logger.writeLog("qryProcLst.size()2=="+qryProcLst.size());
		            //totalComputeTime+=simuQryList.get(selQryIndex).simuPars.curBatchCompTime;
					
		            /*System.out.println(simuQryList.get(selQryIndex).query_id+" Batch:"+simuQryList.get(selQryIndex).simuPars.numBatchesProcessed
				+" processed @ "+simuCurTime+" with Config="+nodeIndex+" curBatchCompTime="+simuQryList.get(selQryIndex).simuPars.curBatchCompTime+" aggCost="+
				simuQryList.get(selQryIndex).simuPars.aggTime+" totalTuples="+simuQryList.get(selQryIndex).num_tuple_total);*/
		            //Logger.writeLog(simuQryList.get(selQryIndex).query_id+" Batch:"+simuQryList.get(selQryIndex).simuPars.numBatchesProcessed+" processed @ "+simuCurTime+" with Config="+nodeIndex+" curBatchCompTime="+simuQryList.get(selQryIndex).simuPars.curBatchCompTime+" aggCost="+
		            //	simuQryList.get(selQryIndex).simuPars.aggTime+" totalTuples="+simuQryList.get(selQryIndex).num_tuple_total+" processed="+simuQryList.get(selQryIndex).simuPars.numTuplesProcessed+" curbchsize="+simuQryList.get(selQryIndex).simuPars.curBatchSize);
		            Logger.writeLog("Processssed "+simuQryListTmp.get(selQryIndex).query_id+" "+simuQryListTmp.get(selQryIndex).simuPars.curBatchReadyTime+" "+simuQryListTmp.get(selQryIndex).simuPars.curBatchCompTime+" "+simuCurTime+" "+simuQryListTmp.get(selQryIndex).simuPars.numTuplesPending+" "+simuQryListTmp.get(selQryIndex).simuPars.numTuplesProcessed+" "+simuQryListTmp.get(selQryIndex).simuPars.curBatchSize+" "+Cluster.numNodes.get(nodeIndex));
		            Logger.writeLog("bchSch size="+bchSch.curTime.size());
		            //System.out.println("Processssed "+simuQryListTmp.get(selQryIndex).query_id+" "+simuQryListTmp.get(selQryIndex).simuPars.curBatchSize+" "+simuQryListTmp.get(selQryIndex).simuPars.numTuplesPending+" @ "+simuCurTime);
		            simuQryListTmp.get(selQryIndex).simuPars.curNumNodes=Cluster.numNodes.get(nodeIndex);
		            simuQryListTmp.get(selQryIndex).simuPars.numBatchesProcessed++;
		            simuQryListTmp.get(selQryIndex).simuPars.curBatchNo++;
		            simuQryListTmp.get(selQryIndex).simuPars.numTuplesProcessed+=simuQryListTmp.get(selQryIndex).simuPars.curBatchSize;							
		            if(simuQryListTmp.get(selQryIndex).simuPars.numTuplesPending<simuQryListTmp.get(selQryIndex).simuPars.curBatchSize) {
				simuQryListTmp.get(selQryIndex).simuPars.curBatchSize=simuQryListTmp.get(selQryIndex).simuPars.numTuplesPending;
				simuQryListTmp.get(selQryIndex).simuPars.numTuplesPending=0;
		            }
		            else {
		                //VerifyValues(simuQryListTmp.get(selQryIndex).simuPars.numTuplesPending,simuQryListTmp.get(selQryIndex).simuPars.curBatchSize,5);
				simuQryListTmp.get(selQryIndex).simuPars.numTuplesPending-=simuQryListTmp.get(selQryIndex).simuPars.curBatchSize;
		            }
		            //System.out.println(simuQryListTmp.get(selQryIndex).query_id+" numTuplesPending=="+simuQryListTmp.get(selQryIndex).simuPars.numTuplesPending+" "+simuQryListTmp.get(selQryIndex).num_tuple_total);
		            //Logger.writeLog("simuCurTime=="+simuCurTime);
		            
		            
		            
		            
		            if(simuQryListTmp.get(selQryIndex).simuPars.numTuplesProcessed>=simuQryListTmp.get(selQryIndex).num_tuple_total) {
		                Logger.writeLog("doing final agg for "+simuQryListTmp.get(selQryIndex).query_id+" @ "+simuCurTime+" "+simuQryListTmp.get(selQryIndex).simuPars.aggTime);
				simuCurTime+=simuQryListTmp.get(selQryIndex).simuPars.aggTime;	
				//System.out.println("size before updating agg time:"+bchSch.compStopTimeLst.size());
		                bchSch.compStopTimeLst.set(bchSch.compStopTimeLst.size()-1,simuCurTime);
		       		//System.out.println("size after updating agg time:"+bchSch.compStopTimeLst.size());
				//Logger.writeLog("Completed Agg for "+simuQryListTmp.get(selQryIndex).query_id+" @ "+simuCurTime);
				//totalComputeTime+=simuQryList.get(selQryIndex).simuPars.aggTime;
				//check if any query has failed 
				
				//simuQryListTmp.remove(selQryIndex);
		                simuQryListTmp.get(selQryIndex).completed=true;
		                //completionSts.add(selQryIndex,1);
		                //schPtValues.completionStsLst.add(schPtValues.simuTime.size()-1,completionSts);
		                //System.out.println("Setting compstoptime::"+(bchSch.compStopTimeLst.get(bchSch.compStopTimeLst.size()-1))+" "+(bchSch.compStopTimeLst.size()-1)+" "+(bchSch.curTime.size()-1));
		            }          
		        
		        /*Logger.writeLog("bchSchsizes:"+bchSch.batchRdyTimeLst.size()+" "+bchSch.compStartTimeLst.size()+" "+bchSch.compStopTimeLst.size()+
		                " "+bchSch.qryBatchNoLst.size()+" "+bchSch.qryIdLst.size()+" "+bchSch.reqNumNodesLst.size());*/
		    }		
				
		                      
		    //return obj;
		}

	
	public static void writeSchToFile() {
		String qryIpBasePath=System.getenv("QRY_INPUT_PATH"); 
		String inputPath=qryIpBasePath+"/";	 
		String fileOp=inputPath+"sch.txt";
		BufferedWriter writer;
	        
	                
		try {
			writer = new BufferedWriter(new FileWriter(fileOp));
		
		String tmp="";
	        //System.out.println(QueryScheduler.multiQrySchLst.nodeReqTimeLst.size());
	        writer.write("Schedule generated at "+QueryScheduler.cur_time);
	        writer.write("\n");
	        for(int m=0;m<QueryScheduler.multiQrySchLst.nodeReqTimeLst.size();m++) {
	            tmp=QueryScheduler.multiQrySchLst.nodeReqTimeLst.get(m)+"\t"+QueryScheduler.multiQrySchLst.qryIdLst.get(m)+"\t"+QueryScheduler.multiQrySchLst.reqNumNodesLst.get(m)+"\t"+QueryScheduler.multiQrySchLst.qryBatchNoLst.get(m);
	            tmp+="\n";
	            writer.write(tmp);
	        }
	    
	        writer.close();        
		}catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	}
        
    public static void writeResToFile(MultiQryBchSch bchSch,List<Query> q_list){        
        for(int i=0;i<bchSch.curTime.size();i++){
            if(bchSch.curTime.get(i)>=4500){
                for(int q=0;q<q_list.size();q++){
                    if(bchSch.qryIdLst.get(i).equals(q_list.get(q).query_id)){
                        if(bchSch.compStopTimeLst.get(i)<=q_list.get(q).deadline)
                            Logger.writeLog("ToLog::"+i+" "+bchSch.qryIdLst.get(i)+" "+bchSch.compStartTimeLst.get(i)+" "+bchSch.compStopTimeLst.get(i)+" "+q_list.get(q).deadline+" "+bchSch.reqNumNodesLst.get(i)+" Deadline Met");        
                        else
                            Logger.writeLog("ToLog::"+i+" "+bchSch.qryIdLst.get(i)+" "+bchSch.compStartTimeLst.get(i)+" "+bchSch.compStopTimeLst.get(i)+" "+q_list.get(q).deadline+" "+bchSch.reqNumNodesLst.get(i)+" Missed Deadline");        
                        break;
                    }
                }                
		//System.out.println(i+" "+bchSch.qryIdLst.get(i)+" "+bchSch.compStartTimeLst.get(i)+" "+bchSch.compStopTimeLst.get(i)+" "+bchSch.reqNumNodesLst.get(i));
            }
	}
    }
}
