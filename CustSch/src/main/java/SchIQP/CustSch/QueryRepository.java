package SchIQP.CustSch;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

public class QueryRepository {


		static String  outputPath = System.getenv("OUTPUT_PATH");
		static String  tpcInputPath=System.getenv("TPC_INPUT_PATH");
		static String  yahooInputPath=System.getenv("YAHOO_INPUT_PATH");
		
		static String 	fullPathCust=tpcInputPath+"/file-ip/static/customer";
		static String	fullPathPart=tpcInputPath+"/file-ip/static/parts";
		static String	fullPathPartSupp=tpcInputPath+"/file-ip/static/partSupp";
		static String 	fullPathSupp=tpcInputPath+"/file-ip/static/supplier";
		static String	fullPathNation=tpcInputPath+"/file-ip/static/nation";
		static String 	fullPathAdCampaings=yahooInputPath+"/staticdata2";
		
			
			
		
		public static void exe_qry(String queryNo, String batchNo, int aggLevel, Map<String,ArrayList<String>> files) {
			
			if(queryNo.equals("Q1"))
				exe_q1(queryNo, batchNo, aggLevel,files);
			else if(queryNo.equals("Q3"))
				exe_q3(queryNo, batchNo, aggLevel,files);
			else if(queryNo.equals("Q4"))
				exe_q4(queryNo, batchNo, aggLevel,files);
			else if(queryNo.equals("Q6"))
				exe_q6(queryNo, batchNo, aggLevel,files);
			else if(queryNo.equals("Q9"))
				exe_q9(queryNo, batchNo, aggLevel,files);
			else if(queryNo.equals("Q10"))
				exe_q10(queryNo, batchNo, aggLevel,files);
			else if(queryNo.equals("Q12"))
				exe_q12(queryNo, batchNo, aggLevel,files);
			else if(queryNo.equals("Q14"))
				exe_q14(queryNo, batchNo, aggLevel,files);
			else if(queryNo.equals("Q19"))
				exe_q19(queryNo, batchNo, aggLevel,files);
			else if(queryNo.equals("YQ1"))
				exe_yq1(queryNo,batchNo,aggLevel,files);
			else
				exe_custQry(queryNo,batchNo, aggLevel,files); 
		}
		
		public  static void exe_startup_qry(Map<String,ArrayList<String>> files) {
			
			
			//String basePathStrtUp=inputPath+"/file-ip/streaming/startup";		
			ArrayList<String> lFiles=files.get("lineitem");	
	        String[] lFiles_str=new String[lFiles.size()];
	        for(int xx=0;xx<lFiles.size();xx++)
					lFiles_str[xx]=lFiles.get(xx);
				
			 StructType lineItemSchema = new StructType()
						.add("orderKey", "integer")
						.add("partKey", "integer")
						.add("suppKey", "integer")
						.add("lineNo", "integer")
						.add("qty", "integer")
						.add("extendedPrice", "float")
						.add("discount", "float")
						.add("tax", "float")
						.add("returnFlg", "string")
						.add("lineStatus","string")
						.add("shipDate", org.apache.spark.sql.types.DataTypes.DateType)
						.add("commitDate", org.apache.spark.sql.types.DataTypes.DateType)
						.add("receiptDate", org.apache.spark.sql.types.DataTypes.DateType)
						.add("shipInstr", "string")
						.add("shipMode", "string")
						.add("comment", "string")
						.add("l_time","string");
				
				Dataset<Row> lineItemDFCsv = CSSparkSession.spark.read().format("csv")
						  .option("sep", "|")
						  .option("inferSchema", "false")
						  .option("header", "false")
						  .schema(lineItemSchema)
						  .load(lFiles_str);
				
			
				lineItemDFCsv.createOrReplaceTempView("lItems");		
				
				String query = "select * from  lItems ";
				
				Dataset<Row> ItemsPerSuppKey = CSSparkSession.spark.sql(query); 
				ItemsPerSuppKey.show();		 		

		}
		
		public  static void exe_yq1(String queryNo, String batchNo, int aggLevel, Map<String,ArrayList<String>> files) {
			
	    		
			
			
			ArrayList<String> yFiles=files.get("events");
			String[] yFiles_str=new String[yFiles.size()];
	        for(int xx=0;xx<yFiles.size();xx++)
					yFiles_str[xx]=yFiles.get(xx);
				
			if((aggLevel==0)||(aggLevel==2)||(aggLevel==3)) {			 
			 StructType eventsSchema = new StructType()
						.add("user_id", "long")
						.add("page_id", "long")
						.add("ad_id", "long")
						.add("ad_type", "string")
						.add("event_type", "string")
						.add("event_time", "timestamp")
						.add("ip_address", "string");
						
				
				/*Dataset<Row> eventsDFCsv = CSSparkSession.spark.read().format("parquet")
						  .option("sep", "|")
						  .option("inferSchema", "false")
						  .option("header", "false")
						  .schema(eventsSchema)
						  .load(yFiles_str);*/
				

				Dataset<Row> eventsDFCsv = CSSparkSession.spark.read().json(yFiles_str);

				eventsDFCsv.createOrReplaceTempView("events");


						
				StructType campaignSchema = new StructType()
						.add("ad_id", "string")
						.add("campaign_id", "string");
						
				
				Dataset<Row> campaignDFCsv = CSSparkSession.spark.read().format("csv")
						  .option("sep", ",")
						  .option("inferSchema", "false")
						  .option("header", "false")
						  .schema(campaignSchema)
						  .load(fullPathAdCampaings+"/ad-to-campaign-ids.csv");
				
				campaignDFCsv.createOrReplaceTempView("campaigns");
				
			
				String query = "select count(events.event_time) as event_cnt, campaign_id from events, campaigns where events.event_type='view' and events.ad_id=campaigns.ad_id group by campaigns.campaign_id";
				
				//String query = "select events.event_time,events.ad_id,campaigns.campaign_id  from events,campaigns where events.event_type='view' and events.ad_id=campaigns.ad_id";
			
				//String query = "select events.event_time,events.ad_id,campaigns.campaign_id  from events,campaigns where events.event_type='view' and events.ad_id=campaigns.ad_id";
				
				//String query = "select * from campaigns";

				//String query = "select events.event_time,events.ad_id  from events events.event_type='view'";


				Dataset<Row> filteredEvents = CSSparkSession.spark.sql(query);
				String opFile=outputPath+"/results/tmp-2/"+queryNo+"/res"+batchNo;
				//Logger.writeLog(opFile);			
				filteredEvents.write().mode("overwrite").csv(opFile); 
				//filteredEvents.show();
							
			}
			 
			  if((aggLevel==1)||(aggLevel==2)||(aggLevel==3)) {
					
					String intrResPath=outputPath+"/results/tmp-2/"+queryNo+"/*/";
					
					//Logger.writeLog("intrResPath=="+intrResPath);
					
					StructType yAggSchema = new StructType()
							.add("event_cnt", "integer")
							//.add("key", "integer");
							.add("campaign_id", "long");


					Dataset<Row> qryDFCsv = CSSparkSession.spark.read().format("csv")
							  .option("sep", ",")
							  .option("inferSchema", "false")
							  .option("header", "false")
							  .schema(yAggSchema)
							  .load(intrResPath+"/*.csv");
						
							qryDFCsv.createOrReplaceTempView("results");
						
					Dataset<Row> aggRes = CSSparkSession.spark.sql("select sum(event_cnt), campaign_id from results group by campaign_id");
						
				
			
				if(aggLevel==3){
					String opFile=outputPath+"/results/tmp-3/"+queryNo+"/res"+batchNo;
					//Logger.writeLog(opFile);			
					aggRes.write().mode("overwrite").csv(opFile); 
				}
				else{				
					String opFile=outputPath+"/results/tmp-4/"+queryNo+"/res"+batchNo;
					//Logger.writeLog(opFile);			
					aggRes.write().mode("overwrite").csv(opFile); 
					Cluster.removeFiles(outputPath+"results/tmp-2/"+queryNo+"/");
				}
					
					
							
					
				}
				
				  if(aggLevel==2) {
					
					String intrResPath=outputPath+"/results/tmp-4/"+queryNo+"/*/";
					
					//Logger.writeLog("intrResPath=="+intrResPath);
					
								
					StructType yAggSchema = new StructType()
							.add("event_cnt", "integer")
							//.add("key", "integer");
							.add("campaign_id", "long");


					Dataset<Row> qryDFCsv = CSSparkSession.spark.read().format("csv")
							  .option("sep", ",")
							  .option("inferSchema", "false")
							  .option("header", "false")
							  .schema(yAggSchema)
							  .load(intrResPath+"/*.csv");
						
							qryDFCsv.createOrReplaceTempView("results");
						
					Dataset<Row> aggRes = CSSparkSession.spark.sql("select sum(event_cnt), campaign_id from results group by campaign_id");
					
					
		            
					String opFile=outputPath+"/results/tmp-3/"+queryNo+"/res"+batchNo;
					Logger.writeLog("ToLog::"+opFile);			
					aggRes.write().mode("overwrite").csv(opFile);
											
					
					
				}


		}

		
		public  static void exe_q1(String queryNo, String batchNo, int aggLevel, Map<String,ArrayList<String>> files) {
			
		    	
			//Logger.writeLog("fullPath2:"+fullPath2);
		   // fullPath1="/home/saranyac/eclipse-workspace/spark/file-ip/streaming/Q1-batch-1/lineitem";  //TRB
			
			ArrayList<String> lFiles=files.get("lineitem");
			String[] lFiles_str=new String[lFiles.size()];
	        for(int xx=0;xx<lFiles.size();xx++)
					lFiles_str[xx]=lFiles.get(xx);
				
			if((aggLevel==0)||(aggLevel==2)||(aggLevel==3)) {			 
			 StructType lineItemSchema = new StructType()
						.add("orderKey", "integer")
						.add("partKey", "integer")
						.add("suppKey", "integer")
						.add("lineNo", "integer")
						.add("qty", "integer")
						.add("extendedPrice", "float")
						.add("discount", "float")
						.add("tax", "float")
						.add("returnFlg", "string")
						.add("lineStatus","string")
						.add("shipDate", org.apache.spark.sql.types.DataTypes.DateType)
						.add("commitDate", org.apache.spark.sql.types.DataTypes.DateType)
						.add("receiptDate", org.apache.spark.sql.types.DataTypes.DateType)
						.add("shipInstr", "string")
						.add("shipMode", "string")
						.add("comment", "string")
						.add("l_time","string");
				
				Dataset<Row> lineItemDFCsv = CSSparkSession.spark.read().format("csv")
						  .option("sep", "|")
						  .option("inferSchema", "false")
						  .option("header", "false")
						  .schema(lineItemSchema)
						  .load(lFiles_str);
				
			
				lineItemDFCsv.createOrReplaceTempView("lItems");


				String query = "select returnFlg, lineStatus, sum(qty) as sum_qty,"
				  		+ "	sum(extendedPrice) as sum_base_price, sum(extendedPrice * (1 - discount)) as sum_disc_price,"
				  		+ "	sum(extendedPrice * (1 - discount) * (1 + tax)) as sum_charge, avg(qty) as avg_qty,"
				  		+ "	avg(extendedPrice) as avg_price, avg(discount) as avg_disc, count(*) as count_order "
				  		+ "	from lItems where shipDate <= date '1998-12-01' - interval '10' day group by returnFlg,"
				  		+ " lineStatus order by returnFlg, lineStatus";
				
				
						
				
				Dataset<Row> ItemsPerSuppKey = CSSparkSession.spark.sql(query); 
				
			
				String opFile=outputPath+"/results/tmp-2/"+queryNo+"/res"+batchNo;
				//Logger.writeLog(opFile);			
	            		ItemsPerSuppKey.write().mode("overwrite").csv(opFile); 
							
			}
			 
			  if((aggLevel==1)||(aggLevel==2)||(aggLevel==3)) {
					
					String intrResPath=outputPath+"/results/tmp-2/"+queryNo+"/*/";
					
					//Logger.writeLog("intrResPath=="+intrResPath);
					
					StructType userSchema = new StructType()
							.add("returnFlg", "string")
							.add("lineStatus","string")
							.add("sum_qty","integer")
							.add("sum_base_price", "double")
							.add("sum_disc_price","double")
							.add("sum_charge","double")
							.add("avg_qty","double")
							.add("avg_price","double")
							.add("avg_disc","double")
							.add("count_order","integer");	
					
					Dataset<Row> ordersDFCsv;
					
					ordersDFCsv = CSSparkSession.spark.read().format("csv")
							  .option("sep", ",")
							  .option("inferSchema", "false")
							  .option("header", "false")
							  .schema(userSchema)
							  .load(intrResPath+"/*.csv");
					
					ordersDFCsv.createOrReplaceTempView("orders");
					
					
					String query = "select returnFlg, lineStatus, sum(sum_qty) as f_sum_qty, sum(sum_base_price) as f_sum_base_price,"
							+ " sum(sum_disc_price) as f_sum_disc_price, sum(sum_charge) as f_sum_charge, count(1) as f_count, "
							+ " (sum(avg_qty)/count(*)) as f_avg_qty,"
							+ " (sum(avg_price)/count(*)) as f_avg_price,"
							+ " (sum(avg_disc)/count(*)) as f_avg_disc, sum(count_order) as f_count_order from orders "
							+ " group by returnFlg, lineStatus ";


						
					
					Dataset<Row> Res = CSSparkSession.spark.sql(query); 
					
				if(aggLevel==3){
					String opFile=outputPath+"/results/tmp-3/"+queryNo+"/res"+batchNo;
					Logger.writeLog("ToLog::"+opFile);			
					Res.write().mode("overwrite").csv(opFile); 
					
				}
				
				else{
					String opFile=outputPath+"/results/tmp-4/"+queryNo+"/res"+batchNo;
					Logger.writeLog("ToLog::"+opFile);			
					Res.write().mode("overwrite").csv(opFile); 
					Cluster.removeFiles(outputPath+"results/tmp-2/"+queryNo+"/");
					}
							
					
				}
				
				  if(aggLevel==2) {
					
					String intrResPath=outputPath+"/results/tmp-4/"+queryNo+"/*/";
					
					//Logger.writeLog("intrResPath=="+intrResPath);
					
					StructType userSchema = new StructType()
							.add("returnFlg", "string")
							.add("lineStatus","string")
							.add("sum_qty","integer")
							.add("sum_base_price", "double")
							.add("sum_disc_price","double")
							.add("sum_charge","double")
							.add("avg_qty","double")
							.add("avg_price","double")
							.add("avg_disc","double")
							.add("count_order","integer");	
					
					Dataset<Row> ordersDFCsv;

					ordersDFCsv = CSSparkSession.spark.read().format("csv")
							  .option("sep", ",")
							  .option("inferSchema", "false")
							  .option("header", "false")
							  .schema(userSchema)
							  .load(intrResPath+"/*.csv");
					
					ordersDFCsv.createOrReplaceTempView("orders");
					
					
					String query = "select returnFlg, lineStatus, sum(sum_qty) as f_sum_qty, sum(sum_base_price) as f_sum_base_price,"
							+ " sum(sum_disc_price) as f_sum_disc_price, sum(sum_charge) as f_sum_charge, count(1) as f_count, "
							+ " (sum(avg_qty)/count(*)) as f_avg_qty,"
							+ " (sum(avg_price)/count(*)) as f_avg_price,"
							+ " (sum(avg_disc)/count(*)) as f_avg_disc, sum(count_order) as f_count_order from orders "
							+ " group by returnFlg, lineStatus ";


						
					
					Dataset<Row> Res = CSSparkSession.spark.sql(query); 
					
					
		            
					String opFile=outputPath+"/results/tmp-3/"+queryNo+"/res"+batchNo;
					Logger.writeLog("ToLog::"+opFile);			
					Res.write().mode("overwrite").csv(opFile);
											
					
					
				}


		}
		
		public static void exe_q3(String queryNo, String batchNo, int aggLevel, Map<String,ArrayList<String>> files) {
					
			
			//Logger.writeLog("fullPath1:"+fullPath1);
			ArrayList<String> oFiles=files.get("orders");
			ArrayList<String> lFiles=files.get("lineitem");
			
			if((aggLevel==0)||(aggLevel==2)||(aggLevel==3)) {
			StructType userSchema = new StructType()
					.add("orderKey", "integer")
					.add("custKey", "integer")
					.add("orderStatus", "string")
					.add("totalPrice", "float")
					.add("orderDate_str", org.apache.spark.sql.types.DataTypes.DateType)
					.add("orderPriority", "string")
					.add("orderClerk", "string")
					.add("shipPriority", "integer")
					.add("comment", "string")
					.add("time","string");
					//.add("time1","string");
			
			String[] oFiles_str=new String[oFiles.size()];
			for(int xx=0;xx<oFiles.size();xx++)
					oFiles_str[xx]=oFiles.get(xx);
		        
			/*for(int xx=0;xx<oFiles.size();xx++)
				Logger.writeLog(oFiles_str[xx]);*/

			Dataset<Row> ordersDFCsv = CSSparkSession.spark.read().format("csv")
					  .option("sep", "|")
					  .option("inferSchema", "false")
					  .option("header", "false")
					  .schema(userSchema)
					  .load(oFiles_str);
			
			//ordersDFCsv.show();
			
			// Register the DataFrame as a SQL temporary view
			ordersDFCsv.createOrReplaceTempView("orders");
			//ordersDFCsv.repartition(16);
			 
			 StructType lineItemSchema = new StructType()
						.add("orderKey", "integer")
						.add("partKey", "integer")
						.add("suppKey", "integer")
						.add("lineNo", "integer")
						.add("qty", "integer")
						.add("extendedPrice", "float")
						.add("discount", "float")
						.add("tax", "float")
						.add("returnFlg", "string")
						.add("lineStatus","string")
						.add("shipDate", org.apache.spark.sql.types.DataTypes.DateType)
						.add("commitDate", org.apache.spark.sql.types.DataTypes.DateType)
						.add("receiptDate", org.apache.spark.sql.types.DataTypes.DateType)
						.add("shipInstr", "string")
						.add("shipMode", "string")
						.add("comment", "string")
						.add("l_time","string");
						//.add("time1","string");
				
			 	String[] lFiles_str=new String[lFiles.size()];
				for(int xx=0;xx<lFiles.size();xx++)
							lFiles_str[xx]=lFiles.get(xx);

				
				/*for(int xx=0;xx<lFiles.size();xx++)
					Logger.writeLog(lFiles_str[xx]);*/
				Dataset<Row> lineItemDFCsv = CSSparkSession.spark.read().format("csv")
						  .option("sep", "|")
						  .option("inferSchema", "false")
						  .option("header", "false")
						  .schema(lineItemSchema)
						  .load(lFiles_str);
				
			
				lineItemDFCsv.createOrReplaceTempView("lItems");
				//lineItemDFCsv.repartition(16);
				StructType customerSchema = new StructType()
						.add("c_custKey", "integer")
						.add("c_name", "string")
						.add("c_address", "string")
						.add("c_nationKey", "integer")
						.add("c_phone", "string")
						.add("c_acctbal", "float")
						.add("c_mktSegment", "string")
						.add("c_comment", "string");
						
				
				Dataset<Row> custDFCsv = CSSparkSession.spark.read().format("csv")
						  .option("sep", "|")
						  .option("inferSchema", "false")
						  .option("header", "false")
						  .schema(customerSchema)
						  .load(fullPathCust+"/*");
				
				custDFCsv.createOrReplaceTempView("customer");
				//custDFCsv.repartition(16);

				// Q14
				String query = "select lItems.orderkey, sum(extendedprice * (1 - discount)) as revenue,	orderDate_str,	"
						+ " shipPriority from customer, orders,	lItems where c_mktSegment = 'HOUSEHOLD'	and c_custkey = orders.custkey "
						+ " and lItems.orderKey = orders.orderkey and orders.orderDate_str < date '1992-02-01' and "
						+ " shipDate > date '1992-02-01' group by lItems.orderKey, orders.orderDate_str, orders.shipPriority"; 
		
		
					
				
				Dataset<Row> Res = CSSparkSession.spark.sql(query); 
				

				String opFile=outputPath+"/results/tmp-2/"+queryNo+"/res"+batchNo;
				//Logger.writeLog(opFile);
	            Res.write().mode("overwrite").csv(opFile); 		 
			}
			  if((aggLevel==1)||(aggLevel==2)||(aggLevel==3)) {
				
					String intrResPath=outputPath+"/results/tmp-2/"+queryNo+"/*/";
								
					StructType userSchema = new StructType()
							.add("orderKey", "integer")
							.add("revenue","float")	
							.add("orderDate_str", org.apache.spark.sql.types.DataTypes.DateType)
							.add("shipPriority", "integer");	
					
					Dataset<Row> ordersDFCsv;
					
					
					ordersDFCsv = CSSparkSession.spark.read().format("csv")
							  .option("sep", ",")
							  .option("inferSchema", "false")
							  .option("header", "false")
							  .schema(userSchema)
							  .load(intrResPath+"/*.csv");
					
					
					ordersDFCsv.createOrReplaceTempView("orders");
					//ordersDFCsv.repartition(4);
					
					String query = "select orderkey, sum(revenue) as final_revenue, orderDate_str, shipPriority from orders group by "
					 		+ "orderKey, orderDate_str, shipPriority ";


						
					
					Dataset<Row>  Res = CSSparkSession.spark.sql(query); 
					
				
				if(aggLevel==3){
					String opFile=outputPath+"/results/tmp-3/"+queryNo+"/res"+batchNo;
					//Logger.writeLog(opFile);			
					Res.write().mode("overwrite").csv(opFile); 
				}
				else{	
					String opFile=outputPath+"/results/tmp-4/"+queryNo+"/res"+batchNo;
					//Logger.writeLog(opFile);			
					Res.write().mode("overwrite").csv(opFile); 
					Cluster.removeFiles(outputPath+"results/tmp-2/"+queryNo+"/");
					}
					
				}
				 if(aggLevel==2) {
				
					String intrResPath=outputPath+"/results/tmp-4/"+queryNo+"/*/";
								
					StructType userSchema = new StructType()
							.add("orderKey", "integer")
							.add("revenue","float")	
							.add("orderDate_str", org.apache.spark.sql.types.DataTypes.DateType)
							.add("shipPriority", "integer");	
					
					Dataset<Row> ordersDFCsv;
					

					ordersDFCsv = CSSparkSession.spark.read().format("csv")
								  .option("sep", ",")
								  .option("inferSchema", "false")
								  .option("header", "false")
								  .schema(userSchema)
								  .load(intrResPath+"/*.csv");
					
					
					ordersDFCsv.createOrReplaceTempView("orders");
					//ordersDFCsv.repartition(4);
					
					String query = "select orderkey, sum(revenue) as final_revenue, orderDate_str, shipPriority from orders group by "
					 		+ "orderKey, orderDate_str, shipPriority ";


						
					
					Dataset<Row>  Res = CSSparkSession.spark.sql(query); 
					
				
					String opFile=outputPath+"/results/tmp-3/"+queryNo+"/res"+batchNo;
					//Logger.writeLog(opFile);			
					Res.write().mode("overwrite").csv(opFile);
					
					
				}
			
		}
		
		public static void exe_q4(String queryNo, String batchNo, int aggLevel, Map<String,ArrayList<String>> files) {
			
			ArrayList<String> oFiles=files.get("orders");
			ArrayList<String> lFiles=files.get("lineitem");
			
			if((aggLevel==0)||(aggLevel==2)||(aggLevel==3)) {
			StructType userSchema = new StructType()
					.add("orderKey", "integer")
					.add("custKey", "integer")
					.add("orderStatus", "string")
					.add("totalPrice", "float")
					.add("orderDate_str", org.apache.spark.sql.types.DataTypes.DateType)
					.add("orderPriority", "string")
					.add("orderClerk", "string")
					.add("shipPriority", "integer")
					.add("comment", "string")
					.add("time","string");
			String[] oFiles_str=new String[oFiles.size()];
			for(int xx=0;xx<oFiles.size();xx++)
					oFiles_str[xx]=oFiles.get(xx);
			
			Dataset<Row> ordersDFCsv = CSSparkSession.spark.read().format("csv")
					  .option("sep", "|")
					  .option("inferSchema", "false")
					  .option("header", "false")
					  .schema(userSchema)
					  .load(oFiles_str);
			
		
			ordersDFCsv.createOrReplaceTempView("orders");
			
			 
			 StructType lineItemSchema = new StructType()
						.add("orderKey", "integer")
						.add("partKey", "integer")
						.add("suppKey", "integer")
						.add("lineNo", "integer")
						.add("qty", "integer")
						.add("extendedPrice", "float")
						.add("discount", "float")
						.add("tax", "float")
						.add("returnFlg", "string")
						.add("lineStatus","string")
						.add("shipDate", org.apache.spark.sql.types.DataTypes.DateType)
						.add("commitDate", org.apache.spark.sql.types.DataTypes.DateType)
						.add("receiptDate", org.apache.spark.sql.types.DataTypes.DateType)
						.add("shipInstr", "string")
						.add("shipMode", "string")
						.add("comment", "string")
						.add("l_time","string");
			 
			    String[] lFiles_str=new String[lFiles.size()];
				for(int xx=0;xx<lFiles.size();xx++)
							lFiles_str[xx]=lFiles.get(xx);
				
				Dataset<Row> lineItemDFCsv = CSSparkSession.spark.read().format("csv")
						  .option("sep", "|")
						  .option("inferSchema", "false")
						  .option("header", "false")
						  .schema(lineItemSchema)
						  .load(lFiles_str);
				
			
				lineItemDFCsv.createOrReplaceTempView("lItems");

					

				// Q4
				
				String query = "select orderPriority,	count(*) as order_count from orders, lItems where orderDate_str >= date '1992-01-12' "
						+ " and orderDate_str < date '1992-01-12' + interval '3' month and "
						+ "  orders.orderKey = lItems.orderkey and commitDate < receiptDate  "
						+ " group by orderPriority";
			
					
				
				Dataset<Row> Res = CSSparkSession.spark.sql(query); 
				String opFile=outputPath+"/results/tmp-2/"+queryNo+"/res"+batchNo;
				//Logger.writeLog(opFile);			
	            Res.write().mode("overwrite").csv(opFile); 
				
			}
			 if((aggLevel==1)||(aggLevel==2)||(aggLevel==3)) {
			  
				
				    String intrResPath=outputPath+"/results/tmp-2/"+queryNo+"/*/";
					
					
					
					StructType userSchema = new StructType()
							.add("orderPriority", "string")
							.add("orderCount","integer")	;	
					
					Dataset<Row> ordersDFCsv;
					
					
					ordersDFCsv = CSSparkSession.spark.read().format("csv")
							  .option("sep", ",")
							  .option("inferSchema", "false")
							  .option("header", "false")
							  .schema(userSchema)
							  .load(intrResPath+"/*.csv");

					ordersDFCsv.createOrReplaceTempView("orders");
					
					
					String query = "select orderPriority,	count(orderCount) as final_order_count from orders group by orderPriority ";

					Dataset<Row> Res = CSSparkSession.spark.sql(query); 
					
					// Print the schema of our aggregation
					//ItemsPerSuppKey.show();
					//queryNo="Q14";

				if(aggLevel==3){			
					String opFile=outputPath+"/results/tmp-3/"+queryNo+"/res"+batchNo;
					//Logger.writeLog(opFile);			
					Res.write().mode("overwrite").csv(opFile); 
				}
				else{
					String opFile=outputPath+"/results/tmp-4/"+queryNo+"/res"+batchNo;
					//Logger.writeLog(opFile);			
					Res.write().mode("overwrite").csv(opFile); 
					Cluster.removeFiles(outputPath+"results/tmp-2/"+queryNo+"/");
				}
				

			}
			
			 if(aggLevel==2) {
			  
				
				    String intrResPath=outputPath+"/results/tmp-4/"+queryNo+"/*/";
					
					
					
					StructType userSchema = new StructType()
							.add("orderPriority", "string")
							.add("orderCount","integer")	;	
					
					Dataset<Row> ordersDFCsv;
					
					ordersDFCsv = CSSparkSession.spark.read().format("csv")
							  .option("sep", ",")
							  .option("inferSchema", "false")
							  .option("header", "false")
							  .schema(userSchema)
							  .load(intrResPath+"/*.csv");
					
					
					ordersDFCsv.createOrReplaceTempView("orders");
					
					
					String query = "select orderPriority,	count(orderCount) as final_order_count from orders group by orderPriority ";

					Dataset<Row> Res = CSSparkSession.spark.sql(query); 
					
					// Print the schema of our aggregation
					//ItemsPerSuppKey.show();
					//queryNo="Q14";
					String opFile=outputPath+"/results/tmp-3/"+queryNo+"/res"+batchNo;
					//Logger.writeLog(opFile);			
					Res.write().mode("overwrite").csv(opFile);
					
					
					
					
					
				

			}

		}
		
		public static void exe_q6(String queryNo, String batchNo, int aggLevel,Map<String,ArrayList<String>> files) {
			
			
			ArrayList<String> lFiles=files.get("lineitem");
			
			if((aggLevel==0)||(aggLevel==2)||(aggLevel==3)) {
			 
			 StructType lineItemSchema = new StructType()
						.add("orderKey", "integer")
						.add("partKey", "integer")
						.add("suppKey", "integer")
						.add("lineNo", "integer")
						.add("qty", "integer")
						.add("extendedPrice", "float")
						.add("discount", "float")
						.add("tax", "float")
						.add("returnFlg", "string")
						.add("lineStatus","string")
						.add("shipDate", org.apache.spark.sql.types.DataTypes.DateType)
						.add("commitDate", org.apache.spark.sql.types.DataTypes.DateType)
						.add("receiptDate", org.apache.spark.sql.types.DataTypes.DateType)
						.add("shipInstr", "string")
						.add("shipMode", "string")
						.add("comment", "string")
						.add("l_time","string");
			 	String[] lFiles_str=new String[lFiles.size()];
				for(int xx=0;xx<lFiles.size();xx++)
							lFiles_str[xx]=lFiles.get(xx);
				
				Dataset<Row> lineItemDFCsv = CSSparkSession.spark.read().format("csv")
						  .option("sep", "|")
						  .option("inferSchema", "false")
						  .option("header", "false")
						  .schema(lineItemSchema)
						  .load(lFiles_str);
				
			
				lineItemDFCsv.createOrReplaceTempView("lItems");

				
				String query = "select sum(extendedPrice * discount) as revenue from	lItems where " 
						+ " shipDate >= '1996-03-30' and shipDate < '1996-04-21' and "
						+ " discount between -0.01 and  0.01 and qty < 3";
				
				
				
				Dataset<Row> ItemsPerSuppKey = CSSparkSession.spark.sql(query); 
				
				String opFile=outputPath+"/results/tmp-2/"+queryNo+"/res"+batchNo;
				//Logger.writeLog(opFile);			
	            ItemsPerSuppKey.write().mode("overwrite").csv(opFile); 
			}
			 if((aggLevel==1)||(aggLevel==2)||(aggLevel==3))
			{
		
				String intrResPath=outputPath+"/results/tmp-2/"+queryNo+"/*/";
				
				
				StructType userSchema = new StructType()
						.add("revenue","float");
				Dataset<Row> ordersDFCsv ;
				
			
				ordersDFCsv = CSSparkSession.spark.read().format("csv")
						  .option("sep", ",")
						  .option("inferSchema", "false")
						  .option("header", "false")
						  .schema(userSchema)
						  .load(intrResPath+"/*.csv");
				
				
				
				ordersDFCsv.createOrReplaceTempView("orders");
				
				
				String queryres = "select sum(revenue) from orders ";


				Dataset<Row> Res = CSSparkSession.spark.sql(queryres); 
				
				// Print the schema of our aggregation
				//ItemsPerSuppKey.show();
				//queryNo="Q14";

				if(aggLevel==3){
					String opFile=outputPath+"/results/tmp-3/"+queryNo+"/res"+batchNo;
					//Logger.writeLog(opFile);			
					Res.write().mode("overwrite").csv(opFile); 			
				}			
				else{        		
					String opFile=outputPath+"/results/tmp-4/"+queryNo+"/res"+batchNo;
					//Logger.writeLog(opFile);			
					Res.write().mode("overwrite").csv(opFile); 
					Cluster.removeFiles(outputPath+"results/tmp-2/"+queryNo+"/");
				}
			} 
			
			if(aggLevel==2)
			{
		
				String intrResPath=outputPath+"/results/tmp-4/"+queryNo+"/*/";
				
				
				StructType userSchema = new StructType()
						.add("revenue","float");
				Dataset<Row> ordersDFCsv ;
				

					ordersDFCsv = CSSparkSession.spark.read().format("csv")
							  .option("sep", ",")
							  .option("inferSchema", "false")
							  .option("header", "false")
							  .schema(userSchema)
							  .load(intrResPath+"/*.csv");
				
				ordersDFCsv.createOrReplaceTempView("orders");
				
				
				String queryres = "select sum(revenue) from orders ";


				Dataset<Row> Res = CSSparkSession.spark.sql(queryres); 
				
				// Print the schema of our aggregation
				//ItemsPerSuppKey.show();
				//queryNo="Q14";
					String opFile=outputPath+"/results/tmp-3/"+queryNo+"/res"+batchNo;
					//Logger.writeLog(opFile);			
					Res.write().mode("overwrite").csv(opFile);
				
			} 
			
		 
		}
		
		public static void exe_q9(String queryNo, String batchNo, int aggLevel, Map<String,ArrayList<String>> files) {

			
			ArrayList<String> oFiles=files.get("orders");
			ArrayList<String> lFiles=files.get("lineitem");
			
			if((aggLevel==0)||(aggLevel==2)||(aggLevel==3)) {
		
			StructType userSchema = new StructType()
					.add("orderKey", "integer")
					.add("custKey", "integer")
					.add("orderStatus", "string")
					.add("totalPrice", "float")
					.add("orderDate_str", org.apache.spark.sql.types.DataTypes.DateType)
					.add("orderPriority", "string")
					.add("orderClerk", "string")
					.add("shipPriority", "integer")
					.add("comment", "string")
					.add("time","string");
			
			String[] oFiles_str=new String[oFiles.size()];
			for(int xx=0;xx<oFiles.size();xx++)
					oFiles_str[xx]=oFiles.get(xx);
		        //Logger.writeLog(oFiles.size()+"\t"+lFiles.size());	
		        
		    
			Dataset<Row> ordersDFCsv = CSSparkSession.spark.read().format("csv")
					  .option("sep", "|")
					  .option("inferSchema", "false")
					  .option("header", "false")
					  .schema(userSchema)
					  .load(oFiles_str);
			
			//ordersDFCsv.show();
			
			// Register the DataFrame as a SQL temporary view
			ordersDFCsv.createOrReplaceTempView("orders");
			
		
			
			
			 
			 StructType lineItemSchema = new StructType()
						.add("orderKey", "integer")
						.add("partKey", "integer")
						.add("suppKey", "integer")
						.add("lineNo", "integer")
						.add("qty", "integer")
						.add("extendedPrice", "float")
						.add("discount", "float")
						.add("tax", "float")
						.add("returnFlg", "string")
						.add("lineStatus","string")
						.add("shipDate", org.apache.spark.sql.types.DataTypes.DateType)
						.add("commitDate", org.apache.spark.sql.types.DataTypes.DateType)
						.add("receiptDate", org.apache.spark.sql.types.DataTypes.DateType)
						.add("shipInstr", "string")
						.add("shipMode", "string")
						.add("comment", "string")
						.add("l_time","string");
				
			 	String[] lFiles_str=new String[lFiles.size()];
				for(int xx=0;xx<lFiles.size();xx++)
							lFiles_str[xx]=lFiles.get(xx);
				//Logger.writeLog(lFiles.get(0));
				Dataset<Row> lineItemDFCsv = CSSparkSession.spark.read().format("csv")
						  .option("sep", "|")
						  .option("inferSchema", "false")
						  .option("header", "false")
						  .schema(lineItemSchema)
						  .load(lFiles_str);
				
			
				lineItemDFCsv.createOrReplaceTempView("lItems");

				
				StructType supplierSchema = new StructType()
						.add("s_suppkey", "string")
						.add("s_name", "string")
						.add("s_address", "string")
						.add("s_nationKey", "integer")
						.add("s_phone", "string")
						.add("s_acctbal", "float")					
						.add("s_comment", "string");
						
				
				Dataset<Row> suppDFCsv = CSSparkSession.spark.read().format("csv")
						  .option("sep", "|")
						  .option("inferSchema", "false")
						  .option("header", "false")
						  .schema(supplierSchema)
						  .load(fullPathSupp+"/*");
				
				suppDFCsv.createOrReplaceTempView("supplier");
				
				
				
				StructType nationSchema = new StructType()					
						.add("n_nationKey", "integer")
						.add("n_name", "string")
						.add("n_regionkey","integer")
						.add("n_comment", "string");
						
				
				Dataset<Row> natDFCsv = CSSparkSession.spark.read().format("csv")
						  .option("sep", "|")
						  .option("inferSchema", "false")
						  .option("header", "false")
						  .schema(nationSchema)
						  .load(fullPathNation+"/*");
				
				natDFCsv.createOrReplaceTempView("nation");
				
				
				
				StructType partsSchema = new StructType()
						.add("p_partkey","integer")
						.add("p_name", "string")					
						.add("p_mfgr", "string")
						.add("p_brand","string")
						.add("p_type","string")
						.add("p_size","integer")
						.add("p_container","string")
						.add("p_retailprice","float")
						.add("p_comment","string");
						
				
				Dataset<Row> partsDFCsv = CSSparkSession.spark.read().format("csv")
						  .option("sep", "|")
						  .option("inferSchema", "false")
						  .option("header", "false")
						  .schema(partsSchema)
						  .load(fullPathPart+"/*");
				
				partsDFCsv.createOrReplaceTempView("part");
				
				StructType partSuppSchema = new StructType()
						.add("ps_partkey","integer")
						.add("ps_suppkey", "integer")					
						.add("ps_availqty", "integer")
						.add("ps_supplycost","float")
						.add("ps_comment","string");
						
				
				Dataset<Row> partSuppDFCsv = CSSparkSession.spark.read().format("csv")
						  .option("sep", "|")
						  .option("inferSchema", "false")
						  .option("header", "false")
						  .schema(partSuppSchema)
						  .load(fullPathPartSupp+"/*");
				
				partSuppDFCsv.createOrReplaceTempView("partSupp");
				

				// Q14
				String query = "select nation, o_year, sum(amount) as sum_profit from ( select n_name as nation,"
						+ "	extract(year from orderDate_str) as o_year, extendedPrice * (1 - discount) - ps_supplycost * qty as amount"
						+ "	from part, supplier, lItems, partsupp, orders, nation where s_suppkey = lItems.suppKey"
						+ "	and ps_suppkey = lItems.suppKey	and ps_partkey = partKey and p_partkey = partKey "
						+ " and orders.orderKey = lItems.orderKey and s_nationkey = n_nationkey and p_name like '%chocolate%' ) as profit"
						+ " group by nation, o_year";
		
					
					
				
				Dataset<Row> Res = CSSparkSession.spark.sql(query); 
				
				String opFile=outputPath+"/results/tmp-2/"+queryNo+"/res"+batchNo;
				//Logger.writeLog(opFile);			
	            Res.write().mode("overwrite").csv(opFile); 
			
			}

			
			  if((aggLevel==1)||(aggLevel==2)||(aggLevel==3)) {
				
				  String intrResPath=outputPath+"/results/tmp-2/"+queryNo+"/*/";
				
				
				StructType userSchema = new StructType()
						.add("nation", "string")
						.add("o_year","string")
						.add("amount","float");	
				
				Dataset<Row> ordersDFCsv ;
				
				ordersDFCsv = CSSparkSession.spark.read().format("csv")
						  .option("sep", ",")
						  .option("inferSchema", "false")
						  .option("header", "false")
						  .schema(userSchema)
						  .load(intrResPath+"/*.csv");
				ordersDFCsv.createOrReplaceTempView("orders");
				
				
				 String query = "select nation, o_year, sum(amount) as final_amount from orders group by nation, o_year ";


					
				
				 Dataset<Row> Res = CSSparkSession.spark.sql(query); 
				
				// Print the schema of our aggregation
				//ItemsPerSuppKey.show();
				//queryNo="Q14";
				

				if(aggLevel==3){
				 	String opFile=outputPath+"/results/tmp-3/"+queryNo+"/res"+batchNo;
					//Logger.writeLog(opFile);			
					Res.write().mode("overwrite").csv(opFile);    		        
				}		        
				else{
				 	String opFile=outputPath+"/results/tmp-4/"+queryNo+"/res"+batchNo;
					//Logger.writeLog(opFile);			
					Res.write().mode("overwrite").csv(opFile); 
	   		                Cluster.removeFiles(outputPath+"results/tmp-2/"+queryNo+"/");
	   		        }
				

			}
			
			 if(aggLevel==2) {
				
				  String intrResPath=outputPath+"/results/tmp-4/"+queryNo+"/*/";
				
				
				StructType userSchema = new StructType()
						.add("nation", "string")
						.add("o_year","string")
						.add("amount","float");	
				
				Dataset<Row> ordersDFCsv ;

					ordersDFCsv = CSSparkSession.spark.read().format("csv")
							  .option("sep", ",")
							  .option("inferSchema", "false")
							  .option("header", "false")
							  .schema(userSchema)
							  .load(intrResPath+"/*.csv");

				ordersDFCsv.createOrReplaceTempView("orders");
				
				
				 String query = "select nation, o_year, sum(amount) as final_amount from orders group by nation, o_year ";


					
				
				 Dataset<Row> Res = CSSparkSession.spark.sql(query); 
				
				// Print the schema of our aggregation
				//ItemsPerSuppKey.show();
				//queryNo="Q14";
					 	String opFile=outputPath+"/results/tmp-3/"+queryNo+"/res"+batchNo;
						//Logger.writeLog(opFile);			
						Res.write().mode("overwrite").csv(opFile);

				

			}
			
			
		}
		
		
		public static void exe_q10(String queryNo, String batchNo, int aggLevel, Map<String,ArrayList<String>> files) {
		
			
			ArrayList<String> oFiles=files.get("orders");
			ArrayList<String> lFiles=files.get("lineitem");
			
			if((aggLevel==0)||(aggLevel==2)||(aggLevel==3)) {
			StructType userSchema = new StructType()
					.add("orderKey", "integer")
					.add("custKey", "integer")
					.add("orderStatus", "string")
					.add("totalPrice", "float")
					.add("orderDate_str", org.apache.spark.sql.types.DataTypes.DateType)
					.add("orderPriority", "string")
					.add("orderClerk", "string")
					.add("shipPriority", "integer")
					.add("comment", "string")
					.add("time","string");
			String[] oFiles_str=new String[oFiles.size()];
			for(int xx=0;xx<oFiles.size();xx++)
					oFiles_str[xx]=oFiles.get(xx);
			Dataset<Row> ordersDFCsv = CSSparkSession.spark.read().format("csv")
					  .option("sep", "|")
					  .option("inferSchema", "false")
					  .option("header", "false")
					  .schema(userSchema)
					  .load(oFiles_str);
			
			//ordersDFCsv.show();
			
			// Register the DataFrame as a SQL temporary view
			ordersDFCsv.createOrReplaceTempView("orders");
			
		
			
			
			 
			 StructType lineItemSchema = new StructType()
						.add("orderKey", "integer")
						.add("partKey", "integer")
						.add("suppKey", "integer")
						.add("lineNo", "integer")
						.add("qty", "integer")
						.add("extendedPrice", "float")
						.add("discount", "float")
						.add("tax", "float")
						.add("returnFlg", "string")
						.add("lineStatus","string")
						.add("shipDate", org.apache.spark.sql.types.DataTypes.DateType)
						.add("commitDate", org.apache.spark.sql.types.DataTypes.DateType)
						.add("receiptDate", org.apache.spark.sql.types.DataTypes.DateType)
						.add("shipInstr", "string")
						.add("shipMode", "string")
						.add("comment", "string")
						.add("l_time","string");
			    String[] lFiles_str=new String[lFiles.size()];
				for(int xx=0;xx<lFiles.size();xx++)
							lFiles_str[xx]=lFiles.get(xx);
				Dataset<Row> lineItemDFCsv = CSSparkSession.spark.read().format("csv")
						  .option("sep", "|")
						  .option("inferSchema", "false")
						  .option("header", "false")
						  .schema(lineItemSchema)
						  .load(lFiles_str);
				
			
				lineItemDFCsv.createOrReplaceTempView("lItems");

				
				StructType customerSchema = new StructType()
						.add("c_custKey", "integer")
						.add("c_name", "string")
						.add("c_address", "string")
						.add("c_nationKey", "integer")
						.add("c_phone", "string")
						.add("c_acctbal", "float")
						.add("c_mktSegment", "string")
						.add("c_comment", "string");
						
				
				Dataset<Row> custDFCsv = CSSparkSession.spark.read().format("csv")
						  .option("sep", "|")
						  .option("inferSchema", "false")
						  .option("header", "false")
						  .schema(customerSchema)
						  .load(fullPathCust+"/*");
				
				custDFCsv.createOrReplaceTempView("customer");
				
				
				
				StructType nationSchema = new StructType()					
						.add("n_nationKey", "integer")
						.add("n_name", "string")
						.add("n_regionkey","integer")
						.add("n_comment", "string");
						
				
				Dataset<Row> natDFCsv = CSSparkSession.spark.read().format("csv")
						  .option("sep", "|")
						  .option("inferSchema", "false")
						  .option("header", "false")
						  .schema(nationSchema)
						  .load(fullPathNation+"/*");
				
				natDFCsv.createOrReplaceTempView("nation");
				
				
				
			
				

				// Q14
				String query = "select c_custkey, c_name,	sum(extendedPrice * (1 - discount)) as revenue, c_acctbal,	n_name,"
						+ " c_address, c_phone, c_comment from customer, orders, lItems, nation where c_custkey = custKey"
						+ "	and lItems.orderKey = orders.orderKey and orderDate_str >= date '1992-01-12' and orderDate_str < date '1992-01-12' "
						+ " + interval '3' month and returnFlg = 'R' and c_nationkey = n_nationkey group by "
						+ "	c_custkey, c_name,	c_acctbal, c_phone, n_name, c_address, c_comment" ;
		

					
				
				Dataset<Row> Res = CSSparkSession.spark.sql(query); 
				
				String opFile=outputPath+"/results/tmp-2/"+queryNo+"/res"+batchNo;
				//Logger.writeLog(opFile);			
	            Res.write().mode("overwrite").csv(opFile); 

			}
			
			 if((aggLevel==1)||(aggLevel==2)||(aggLevel==3)) {
				
				String intrResPath=outputPath+"/results/tmp-2/"+queryNo+"/*/";		
				
				StructType userSchema = new StructType()
						.add("c_custkey", "integer")
						.add("c_name","string")
						.add("revenue","float")
						.add("c_acctbal","float")
						.add("n_name","string")
						.add("c_address","string")
						.add("c_phone","string")
						.add("c_comment","string");	
				Dataset<Row>  ordersDFCsv;
				
				ordersDFCsv = CSSparkSession.spark.read().format("csv")
						  .option("sep", ",")
						  .option("inferSchema", "false")
						  .option("header", "false")
						  .schema(userSchema)
						  .load(intrResPath+"/*.csv");
				
				ordersDFCsv.createOrReplaceTempView("orders");
				
				
				String query = "select c_custkey, c_name,	sum(revenue) as final_revenue, c_acctbal,	n_name " 
				+ " c_address, c_phone, c_comment from orders group by c_custkey, c_name,	c_acctbal, c_phone, n_name, "
				+ " c_address, c_comment ";


					
				
				Dataset<Row> Res = CSSparkSession.spark.sql(query); 
				
				// Print the schema of our aggregation
				//ItemsPerSuppKey.show();
				//queryNo="Q14";

				if(aggLevel==3){
					String opFile=outputPath+"/results/tmp-3/"+queryNo+"/res"+batchNo;
					//Logger.writeLog(opFile);			
					Res.write().mode("overwrite").csv(opFile); 
				}				
				else
				{
					String opFile=outputPath+"/results/tmp-4/"+queryNo+"/res"+batchNo;
					//Logger.writeLog(opFile);			
					Res.write().mode("overwrite").csv(opFile); 
					Cluster.removeFiles(outputPath+"results/tmp-2/"+queryNo+"/");
				}
				
				
				

			}
			 if(aggLevel==2) {
				
				String intrResPath=outputPath+"/results/tmp-4/"+queryNo+"/*/";		
				
				StructType userSchema = new StructType()
						.add("c_custkey", "integer")
						.add("c_name","string")
						.add("revenue","float")
						.add("c_acctbal","float")
						.add("n_name","string")
						.add("c_address","string")
						.add("c_phone","string")
						.add("c_comment","string");	
				Dataset<Row>  ordersDFCsv;
				
				ordersDFCsv = CSSparkSession.spark.read().format("csv")
						  .option("sep", ",")
						  .option("inferSchema", "false")
						  .option("header", "false")
						  .schema(userSchema)
						  .load(intrResPath+"/*.csv");
				
				ordersDFCsv.createOrReplaceTempView("orders");
				
				
				String query = "select c_custkey, c_name,	sum(revenue) as final_revenue, c_acctbal,	n_name " 
				+ " c_address, c_phone, c_comment from orders group by c_custkey, c_name,	c_acctbal, c_phone, n_name, "
				+ " c_address, c_comment ";


					
				
				Dataset<Row> Res = CSSparkSession.spark.sql(query); 
				
				
				String opFile=outputPath+"/results/tmp-3/"+queryNo+"/res"+batchNo;
				//Logger.writeLog(opFile);			
				Res.write().mode("overwrite").csv(opFile);

			}
		}
		
		public static void exe_q12(String queryNo, String batchNo, int aggLevel, Map<String,ArrayList<String>> files) {
			
			
				ArrayList<String> oFiles=files.get("orders");
				ArrayList<String> lFiles=files.get("lineitem");
			
				if((aggLevel==0)||(aggLevel==2)||(aggLevel==3)) {
				StructType userSchema = new StructType()
						.add("orderKey", "integer")
						.add("custKey", "integer")
						.add("orderStatus", "string")
						.add("totalPrice", "float")
						.add("orderDate_str", org.apache.spark.sql.types.DataTypes.DateType)
						.add("orderPriority", "string")
						.add("orderClerk", "string")
						.add("shipPriority", "integer")
						.add("comment", "string")
						.add("time","string");
				String[] oFiles_str=new String[oFiles.size()];
				for(int xx=0;xx<oFiles.size();xx++)
						oFiles_str[xx]=oFiles.get(xx);
				Dataset<Row> ordersDFCsv = CSSparkSession.spark.read().format("csv")
						  .option("sep", "|")
						  .option("inferSchema", "false")
						  .option("header", "false")
						  .schema(userSchema)
						  .load(oFiles_str);
				
				//ordersDFCsv.show();
				
				// Register the DataFrame as a SQL temporary view
				ordersDFCsv.createOrReplaceTempView("orders");
				
			
				 
				 StructType lineItemSchema = new StructType()
							.add("orderKey", "integer")
							.add("partKey", "integer")
							.add("suppKey", "integer")
							.add("lineNo", "integer")
							.add("qty", "integer")
							.add("extendedPrice", "float")
							.add("discount", "float")
							.add("tax", "float")
							.add("returnFlg", "string")
							.add("lineStatus","string")
							.add("shipDate", org.apache.spark.sql.types.DataTypes.DateType)
							.add("commitDate", org.apache.spark.sql.types.DataTypes.DateType)
							.add("receiptDate", org.apache.spark.sql.types.DataTypes.DateType)
							.add("shipInstr", "string")
							.add("shipMode", "string")
							.add("comment", "string")
							.add("l_time","string");
				 	String[] lFiles_str=new String[lFiles.size()];
					for(int xx=0;xx<lFiles.size();xx++)
								lFiles_str[xx]=lFiles.get(xx);
					
					Dataset<Row> lineItemDFCsv = CSSparkSession.spark.read().format("csv")
							  .option("sep", "|")
							  .option("inferSchema", "false")
							  .option("header", "false")
							  .schema(lineItemSchema)
							  .load(lFiles_str);
					
				
					lineItemDFCsv.createOrReplaceTempView("lItems");
				

				// Q14
				String query = "select shipMode,	sum(case when orderPriority = '1-URGENT' or orderPriority = '2-HIGH' " 
				+"	then 1	else 0	end) as high_line_count, sum(case when orderPriority <> '1-URGENT'"
				+"	and orderPriority <> '2-HIGH' then 1 else 0	end) as low_line_count"
				+" from orders, lItems where	orders.orderKey = lItems.orderkey and shipMode in ('AIR', 'SHIP')" 
				+"	and commitDate < receiptDate	and shipDate < commitDate and receiptDate >= date '1992-01-12'"
				+"	and receiptDate < date '1992-01-12' + interval '1' year group by shipMode";
		
				
				Dataset<Row> Res = CSSparkSession.spark.sql(query); 
				
				String opFile=outputPath+"/results/tmp-2/"+queryNo+"/res"+batchNo;
				//Logger.writeLog(opFile);			
	            Res.write().mode("overwrite").csv(opFile); 
			  
				}
			
				if((aggLevel==1)||(aggLevel==2)||(aggLevel==3)) {
				 
					String intrResPath=outputPath+"/results/tmp-2/"+queryNo+"/*/";
					
					
					StructType userSchema = new StructType()
							.add("shipMode", "string")
							.add("highCount","integer")	
							.add("lowCount","integer");	
					Dataset<Row> ordersDFCsv;
					
			
					 ordersDFCsv = CSSparkSession.spark.read().format("csv")
							  .option("sep", ",")
							  .option("inferSchema", "false")
							  .option("header", "false")
							  .schema(userSchema)
							  .load(intrResPath+"/*.csv");
					ordersDFCsv.createOrReplaceTempView("orders");
					
					
					String query = "select shipMode,	sum(highCount) as final_highCount, sum(lowCount) as final_lowCount"
					+" from orders group by shipMode  ";


						
					
					 Dataset<Row> Res = CSSparkSession.spark.sql(query); 
					
					// Print the schema of our aggregation
					//ItemsPerSuppKey.show();
					//queryNo="Q14";

					if(aggLevel==3){
					 	String opFile=outputPath+"/results/tmp-3/"+queryNo+"/res"+batchNo;
						//Logger.writeLog(opFile);			
						Res.write().mode("overwrite").csv(opFile); 		                
					}		
					else{
					 	String opFile=outputPath+"/results/tmp-4/"+queryNo+"/res"+batchNo;
						//Logger.writeLog(opFile);			
						Res.write().mode("overwrite").csv(opFile); 
			                         Cluster.removeFiles(outputPath+"results/tmp-2/"+queryNo+"/");
			                }

			}
			if(aggLevel==2) {
				 
					String intrResPath=outputPath+"/results/tmp-4/"+queryNo+"/*/";
					
					
					StructType userSchema = new StructType()
							.add("shipMode", "string")
							.add("highCount","integer")	
							.add("lowCount","integer");	
					Dataset<Row> ordersDFCsv;
					
					
					 ordersDFCsv = CSSparkSession.spark.read().format("csv")
							  .option("sep", ",")
							  .option("inferSchema", "false")
							  .option("header", "false")
							  .schema(userSchema)
							  .load(intrResPath+"/*.csv");
					
					ordersDFCsv.createOrReplaceTempView("orders");
					
					
					String query = "select shipMode,	sum(highCount) as final_highCount, sum(lowCount) as final_lowCount"
					+" from orders group by shipMode  ";


						
					
					 Dataset<Row> Res = CSSparkSession.spark.sql(query); 
					
					// Print the schema of our aggregation
					//ItemsPerSuppKey.show();
					//queryNo="Q14";
					
						 	String opFile=outputPath+"/results/tmp-3/"+queryNo+"/res"+batchNo;
							//Logger.writeLog(opFile);			
							Res.write().mode("overwrite").csv(opFile);
					 

			}
		}
	public static void exe_q14(String queryNo, String batchNo, int aggLevel, Map<String,ArrayList<String>> files) {
			
		
				ArrayList<String> lFiles=files.get("lineitem");
		
				if((aggLevel==0)||(aggLevel==2)||(aggLevel==3)) {
				 
				 StructType lineItemSchema = new StructType()
							.add("orderKey", "integer")
							.add("partKey", "integer")
							.add("suppKey", "integer")
							.add("lineNo", "integer")
							.add("qty", "integer")
							.add("extendedPrice", "float")
							.add("discount", "float")
							.add("tax", "float")
							.add("returnFlg", "string")
							.add("lineStatus","string")
							.add("shipDate", org.apache.spark.sql.types.DataTypes.DateType)
							.add("commitDate", org.apache.spark.sql.types.DataTypes.DateType)
							.add("receiptDate", org.apache.spark.sql.types.DataTypes.DateType)
							.add("shipInstr", "string")
							.add("shipMode", "string")
							.add("comment", "string")
							.add("l_time","string");
				 	String[] lFiles_str=new String[lFiles.size()];
					for(int xx=0;xx<lFiles.size();xx++)
								lFiles_str[xx]=lFiles.get(xx); 
					Dataset<Row> lineItemDFCsv = CSSparkSession.spark.read().format("csv")
							  .option("sep", "|")
							  .option("inferSchema", "false")
							  .option("header", "false")
							  .schema(lineItemSchema)
							  .load(lFiles_str);
					
				
					lineItemDFCsv.createOrReplaceTempView("lItems");

					StructType partsSchema = new StructType()
							.add("p_partkey","integer")
							.add("p_name", "string")					
							.add("p_mfgr", "string")
							.add("p_brand","string")
							.add("p_type","string")
							.add("p_size","integer")
							.add("p_container","string")
							.add("p_retailprice","float")
							.add("p_comment","string");
					
					
					Dataset<Row> partsDFCsv = CSSparkSession.spark.read().format("csv")
							  .option("sep", "|")
							  .option("inferSchema", "false")
							  .option("header", "false")
							  .schema(partsSchema)
							  .load(fullPathPart+"/*");
					
					partsDFCsv.createOrReplaceTempView("part");
					

					// Q14
					String query = "select 100.00 * sum(case when p_type like 'PROMO%' then extendedPrice * (1 - discount)"
					+" else 0 end) / sum(extendedPrice * (1 - discount)) as promo_revenue from	lItems, part "
					+" where  partkey = p_partkey and shipDate >= date '1996-01-01' and shipDate < date '1996-01-01' + interval '1' month";
			
			
					
					Dataset<Row> Res = CSSparkSession.spark.sql(query); 
					
					String opFile=outputPath+"/results/tmp-2/"+queryNo+"/res"+batchNo;
					//Logger.writeLog(opFile);			
		            Res.write().mode("overwrite").csv(opFile); 
				
				}

			
				 if((aggLevel==1)||(aggLevel==2)||(aggLevel==3)) {
					 String intrResPath=outputPath+"/results/tmp-2/"+queryNo+"/*/";
				
				StructType userSchema = new StructType()
						.add("revenue","float");
				
				Dataset<Row> ordersDFCsv;
				
				ordersDFCsv = CSSparkSession.spark.read().format("csv")
						  .option("sep", ",")
						  .option("inferSchema", "false")
						  .option("header", "false")
						  .schema(userSchema)
						  .load(intrResPath+"/*.csv");
				
				ordersDFCsv.createOrReplaceTempView("orders");
				
				
				String query = "select sum(revenue) from orders ";


					
				
				Dataset<Row>  Res = CSSparkSession.spark.sql(query); 
				
				// Print the schema of our aggregation
				//ItemsPerSuppKey.show();
				//queryNo="Q14";
				

				if(aggLevel==3){
					String opFile=outputPath+"/results/tmp-3/"+queryNo+"/res"+batchNo;
					//Logger.writeLog(opFile);			
					Res.write().mode("overwrite").csv(opFile); 			
				}			
				else{
					String opFile=outputPath+"/results/tmp-4/"+queryNo+"/res"+batchNo;
					//Logger.writeLog(opFile);			
					Res.write().mode("overwrite").csv(opFile); 
					Cluster.removeFiles(outputPath+"results/tmp-2/"+queryNo+"/");
				}

			}
			 if(aggLevel==2) {
					 String intrResPath=outputPath+"/results/tmp-4/"+queryNo+"/*/";
				
				StructType userSchema = new StructType()
						.add("revenue","float");
				
				Dataset<Row> ordersDFCsv;

					ordersDFCsv = CSSparkSession.spark.read().format("csv")
							  .option("sep", ",")
							  .option("inferSchema", "false")
							  .option("header", "false")
							  .schema(userSchema)
							  .load(intrResPath+"/*.csv");

				ordersDFCsv.createOrReplaceTempView("orders");
				
				
				String query = "select sum(revenue) from orders ";


					
				
				Dataset<Row>  Res = CSSparkSession.spark.sql(query); 
				
				// Print the schema of our aggregation
				//ItemsPerSuppKey.show();
				//queryNo="Q14";
				
					String opFile=outputPath+"/results/tmp-3/"+queryNo+"/res"+batchNo;
					//Logger.writeLog(opFile);			
					Res.write().mode("overwrite").csv(opFile);
				 
			}
		}
	public static void exe_q19(String queryNo, String batchNo, int aggLevel, Map<String,ArrayList<String>> files) {
		
			 
				
				ArrayList<String> lFiles=files.get("lineitem");
		
				if((aggLevel==0)||(aggLevel==2)||(aggLevel==3)) {
				 StructType lineItemSchema = new StructType()
							.add("orderKey", "integer")
							.add("partKey", "integer")
							.add("suppKey", "integer")
							.add("lineNo", "integer")
							.add("qty", "integer")
							.add("extendedPrice", "float")
							.add("discount", "float")
							.add("tax", "float")
							.add("returnFlg", "string")
							.add("lineStatus","string")
							.add("shipDate", org.apache.spark.sql.types.DataTypes.DateType)
							.add("commitDate", org.apache.spark.sql.types.DataTypes.DateType)
							.add("receiptDate", org.apache.spark.sql.types.DataTypes.DateType)
							.add("shipInstr", "string")
							.add("shipMode", "string")
							.add("comment", "string")
							.add("l_time","string");
				 	String[] lFiles_str=new String[lFiles.size()];
					for(int xx=0;xx<lFiles.size();xx++)
								lFiles_str[xx]=lFiles.get(xx);
					Dataset<Row> lineItemDFCsv = CSSparkSession.spark.read().format("csv")
							  .option("sep", "|")
							  .option("inferSchema", "false")
							  .option("header", "false")
							  .schema(lineItemSchema)
							  .load(lFiles_str);
					
				
					lineItemDFCsv.createOrReplaceTempView("lItems");

					StructType partsSchema = new StructType()
							.add("p_partkey","integer")
							.add("p_name", "string")					
							.add("p_mfgr", "string")
							.add("p_brand","string")
							.add("p_type","string")
							.add("p_size","integer")
							.add("p_container","string")
							.add("p_retailprice","float")
							.add("p_comment","string");
					
					
					Dataset<Row> partsDFCsv = CSSparkSession.spark.read().format("csv")
							  .option("sep", "|")
							  .option("inferSchema", "false")
							  .option("header", "false")
							  .schema(partsSchema)
							  .load(fullPathPart+"/*");
					
					partsDFCsv.createOrReplaceTempView("part");
					

					// Q19
					String query = "select sum(extendedPrice* (1 - discount)) as revenue from lItems, part where (" 
					+ " p_partkey = partKey and p_brand = 'Brand#24' and p_container in ('SM CASE', 'SM BOX', 'SM PACK',"
					+ " 'SM PKG') and qty >= 20 and qty <= 20 + 10 and p_size between 1 and 5 and shipMode in ('AIR', 'AIR REG')"
					+ " and shipInstr = 'DELIVER IN PERSON') or (p_partkey = partKey and p_brand = 'Brand#11'"
					+ " and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK') and qty >= 30 and qty <= 30 + 10 "
					+ " and p_size between 1 and 10	and shipMode in ('AIR', 'AIR REG')	and shipInstr = 'DELIVER IN PERSON' ) "
					+ " or (p_partkey = partKey	and p_brand = 'Brand#43' and p_container in ('LG CASE', 'LG BOX', 'LG PACK',"
					+ " 'LG PKG') and qty >= 25 and qty <= 25 + 10 and p_size between 1 and 15 "
					+ " and shipMode in ('AIR', 'AIR REG') and shipInstr = 'DELIVER IN PERSON')";
			
					
				
					
					Dataset<Row> Res = CSSparkSession.spark.sql(query); 
					
					String opFile=outputPath+"/results/tmp-2/"+queryNo+"/res"+batchNo;
					//Logger.writeLog(opFile);			
		            Res.write().mode("overwrite").csv(opFile); 
				  
				}

		
				 if((aggLevel==1)||(aggLevel==2)||(aggLevel==3)) {
			
					 String intrResPath=outputPath+"/results/tmp-2/"+queryNo+"/*/";
			
			
			StructType userSchema = new StructType()
					.add("revenue","float");
			Dataset<Row> ordersDFCsv;
			
			
			ordersDFCsv = CSSparkSession.spark.read().format("csv")
					  .option("sep", ",")
					  .option("inferSchema", "false")
					  .option("header", "false")
					  .schema(userSchema)
					  .load(intrResPath+"/*.csv");
			
			ordersDFCsv.createOrReplaceTempView("orders");
			
			
			String query = "select sum(revenue) from orders ";


				
			
			Dataset<Row>  Res = CSSparkSession.spark.sql(query); 
			
			// Print the schema of our aggregation
			//ItemsPerSuppKey.show();
			//queryNo="Q14";
			
			if(aggLevel==3){
				String opFile=outputPath+"/results/tmp-3/"+queryNo+"/res"+batchNo;
				//Logger.writeLog(opFile);			
				Res.write().mode("overwrite").csv(opFile); 
			}		
			else{
				String opFile=outputPath+"/results/tmp-4/"+queryNo+"/res"+batchNo;
				//Logger.writeLog(opFile);			
				Res.write().mode("overwrite").csv(opFile); 
				Cluster.removeFiles(outputPath+"results/tmp-2/"+queryNo+"/");
			}
			
		}
		 if(aggLevel==2) {
			
					 String intrResPath=outputPath+"/results/tmp-4/"+queryNo+"/*/";
			
			
			StructType userSchema = new StructType()
					.add("revenue","float");
			Dataset<Row> ordersDFCsv;
			

				ordersDFCsv = CSSparkSession.spark.read().format("csv")
						  .option("sep", ",")
						  .option("inferSchema", "false")
						  .option("header", "false")
						  .schema(userSchema)
						  .load(intrResPath+"/*.csv");

			ordersDFCsv.createOrReplaceTempView("orders");
			
			
			String query = "select sum(revenue) from orders ";


				
			
			Dataset<Row>  Res = CSSparkSession.spark.sql(query); 
			
			// Print the schema of our aggregation
			//ItemsPerSuppKey.show();
			//queryNo="Q14";
				String opFile=outputPath+"/results/tmp-3/"+queryNo+"/res"+batchNo;
				//Logger.writeLog(opFile);			
				Res.write().mode("overwrite").csv(opFile);


		}
	}

	public static void exe_custQry(String queryNo, String batchNo, int aggLevel, Map<String,ArrayList<String>> files) {
		
		
		ArrayList<String> oFiles=files.get("orders");
		ArrayList<String> lFiles=files.get("lineitem");
		
		String queryNoInt= queryNo.substring(2);
		//Logger.writeLog("fullPath1:"+fullPath1);
		if((aggLevel==0)||(aggLevel==2)||(aggLevel==3)) {
		if((Integer.parseInt(queryNoInt)==0)||(Integer.parseInt(queryNoInt)==3)) {

		StructType userSchema = new StructType()
				.add("orderKey", "integer")
				.add("custKey", "integer")
				.add("orderStatus", "string")
				.add("totalPrice", "float")
				.add("orderDate_str", org.apache.spark.sql.types.DataTypes.DateType)
				.add("orderPriority", "string")
				.add("orderClerk", "string")
				.add("shipPriority", "integer")
				.add("comment", "string")
				.add("time","string");
		String[] oFiles_str=new String[oFiles.size()];
		for(int xx=0;xx<oFiles.size();xx++)
				oFiles_str[xx]=oFiles.get(xx);
		Dataset<Row> ordersDFCsv = CSSparkSession.spark.read().format("csv")
				  .option("sep", "|")
				  .option("inferSchema", "false")
				  .option("header", "false")
				  .schema(userSchema)
				  .load(oFiles_str);
		
		//ordersDFCsv.show();
		
		// Register the DataFrame as a SQL temporary view
		ordersDFCsv.createOrReplaceTempView("orders");
		
	}
		
	else {
		 
		 StructType lineItemSchema = new StructType()
					.add("orderKey", "integer")
					.add("partKey", "integer")
					.add("suppKey", "integer")
					.add("lineNo", "integer")
					.add("qty", "integer")
					.add("extendedPrice", "float")
					.add("discount", "float")
					.add("tax", "float")
					.add("returnFlg", "string")
					.add("lineStatus","string")
					.add("shipDate", org.apache.spark.sql.types.DataTypes.DateType)
					.add("commitDate", org.apache.spark.sql.types.DataTypes.DateType)
					.add("receiptDate", org.apache.spark.sql.types.DataTypes.DateType)
					.add("shipInstr", "string")
					.add("shipMode", "string")
					.add("comment", "string")
					.add("l_time","string");
		    String[] lFiles_str=new String[lFiles.size()];
			for(int xx=0;xx<lFiles.size();xx++)
						lFiles_str[xx]=lFiles.get(xx);
			Dataset<Row> lineItemDFCsv = CSSparkSession.spark.read().format("csv")
					  .option("sep", "|")
					  .option("inferSchema", "false")
					  .option("header", "false")
					  .schema(lineItemSchema)
					  .load(lFiles_str);
			
		
			lineItemDFCsv.createOrReplaceTempView("lItems");

			
		}

		

			ArrayList<String> queryLst = new ArrayList<String>();
			queryLst.add("select count(*) as total_orders from orders where orderDate_str='1992-01-12'");
			queryLst.add("select count(*) as totalItems, partKey  from lItems group by partKey");
			//queryLst.add("select count(*) as totalItems, shipMode  from lItems group by shipMode");
			queryLst.add("select count(*) as totalItems, suppKey from lItems group by suppKey");
			//queryLst.add("select count(*) as totalItems from lItems ");
			queryLst.add("select count(*) as total_orders, orderPriority from orders group by orderPriority");
			
			
				
			
			Dataset<Row> Res = CSSparkSession.spark.sql(queryLst.get(Integer.parseInt(queryNoInt))); 
			
			
			String opFile=outputPath+"/results/tmp-2/"+queryNo+"/res"+batchNo;
			//Logger.writeLog(opFile);			
	        Res.write().mode("overwrite").csv(opFile); 
		}
		  if((aggLevel==1)||(aggLevel==2)||(aggLevel==3)) {
				Dataset<Row> aggRes ;
				Dataset<Row> qry12DFCsv;
				String intrResPath=outputPath+"/results/tmp-2/"+queryNo+"/res*/";
				
					//Logger.writeLog("intrResPath=="+intrResPath);
				switch(Integer.parseInt(queryNoInt)) {
				case 0: 
					
					
					
					StructType c1Schema = new StructType()
									.add("totallItmes", "integer");
					
					
					qry12DFCsv = CSSparkSession.spark.read().format("csv")
							  .option("sep", ",")
							  .option("inferSchema", "false")
							  .option("header", "false")
							  .schema(c1Schema)
							  .load(intrResPath+"/*.csv");
					
							qry12DFCsv.createOrReplaceTempView("results");
					
							aggRes = CSSparkSession.spark.sql("select sum(totallItmes) from results ");
						
					
							if(aggLevel==3){
								String opFile=outputPath+"/results/tmp-3/"+queryNo+"/res"+batchNo;
								//Logger.writeLog(opFile);			
								aggRes.write().mode("overwrite").csv(opFile); 
							}
							else{
	        						String opFile=outputPath+"/results/tmp-4/"+queryNo+"/res"+batchNo;
								//Logger.writeLog(opFile);			
								aggRes.write().mode("overwrite").csv(opFile); 
								Cluster.removeFiles(outputPath+"results/tmp-2/"+queryNo+"/");
							}
							break;	
				 case 3: case 2: case 1:
					StructType c2Schema = new StructType()
							.add("totallItmes", "integer")
							//.add("key", "integer");
							.add("key", "string");


							qry12DFCsv = CSSparkSession.spark.read().format("csv")
							  .option("sep", ",")
							  .option("inferSchema", "false")
							  .option("header", "false")
							  .schema(c2Schema)
							  .load(intrResPath+"/*.csv");
						
							qry12DFCsv.createOrReplaceTempView("results");
						
							aggRes = CSSparkSession.spark.sql("select sum(totallItmes), key from results group by key");
						
	         				if(aggLevel==3){
		         				String opFile=outputPath+"/results/tmp-3/"+queryNo+"/res"+batchNo;
							//Logger.writeLog(opFile);			
							aggRes.write().mode("overwrite").csv(opFile); 					
	         				}
						else{
							String opFile=outputPath+"/results/tmp-4/"+queryNo+"/res"+batchNo;
							//Logger.writeLog(opFile);			
							aggRes.write().mode("overwrite").csv(opFile); 
							Cluster.removeFiles(outputPath+"results/tmp-2/"+queryNo+"/");
						}
							break;	
				}
				
		  }


	 if(aggLevel==2) {
				Dataset<Row> aggRes ;
				Dataset<Row> qry12DFCsv;
				String intrResPath=outputPath+"/results/tmp-4/"+queryNo+"/res*/";
				
					//Logger.writeLog("intrResPath=="+intrResPath);
				switch(Integer.parseInt(queryNoInt)) {
				case 0: 
					
					
					
					StructType c1Schema = new StructType()
									.add("totallItmes", "integer");
					
					qry12DFCsv = CSSparkSession.spark.read().format("csv")
							  .option("sep", ",")
							  .option("inferSchema", "false")
							  .option("header", "false")
							  .schema(c1Schema)
							  .load(intrResPath+"/*.csv");
							qry12DFCsv.createOrReplaceTempView("results");
					
							aggRes = CSSparkSession.spark.sql("select sum(totallItmes) from results ");
						
								String opFile=outputPath+"/results/tmp-3/"+queryNo+"/res"+batchNo;
								//Logger.writeLog(opFile);			
								aggRes.write().mode("overwrite").csv(opFile);

							break;	
				 case 3: case 2: case 1:
					StructType c2Schema = new StructType()
							.add("totallItmes", "integer")
							//.add("key", "integer");
							.add("key", "string");

						qry12DFCsv = CSSparkSession.spark.read().format("csv")
								  .option("sep", ",")
								  .option("inferSchema", "false")
								  .option("header", "false")
								  .schema(c2Schema)
								  .load(intrResPath+"/*.csv");

							qry12DFCsv.createOrReplaceTempView("results");
						
							aggRes = CSSparkSession.spark.sql("select sum(totallItmes), key from results group by key");
							opFile=outputPath+"/results/tmp-3/"+queryNo+"/res"+batchNo;
							//Logger.writeLog(opFile);			
							aggRes.write().mode("overwrite").csv(opFile);

							break;	
				}
				
		  }
		

	}

	}
