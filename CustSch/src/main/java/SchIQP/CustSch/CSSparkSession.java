package SchIQP.CustSch;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

public class CSSparkSession {

	static SparkConf conf;
	public static SparkSession spark;
	public static boolean sessionClosed=true;
	static String logPath=System.getenv("LOG_PATH");
	//static String logFile="file:///"+logPath+"/spark-events";
	static String logFile="file:///s3://cs-spark-emr-s3/logs/spark-events/";
	public static void create_spark_session(String numInstances,String numCores){
	 conf = new SparkConf()//.set("spark.driver.memory","10g")
			//.set("spark.executor.memory","10g")
			.set("spark.master.rest.enabled", "true")
			//.set("spark.cores.max","1")
			//.set("spark.history.fs.logDirectory",logFile)
			//.set("spark.eventLog.dir",logFile)
			.set("spark.eventLog.enabled","true")
			.set("spark.history.fs.update.interval","1000")
			.set("spark.executor.instances", numInstances)
			.set("spark.executor.cores", numCores)
			//.set("spark.submit.deployMode","cluster");
			.set("spark.dynamicAllocation.enabled","false");
			//.set("spark.dynamicAllocation.minExecutors","2")
			//.set("spark.dynamicAllocation.maxExecutors","4");
	
	 spark = SparkSession
			  .builder()
			  .master("local[*]")
			  .config(conf)				  
			  .getOrCreate();
	 spark.sparkContext().setLogLevel("ERROR");
	 sessionClosed=false;
	}
}
