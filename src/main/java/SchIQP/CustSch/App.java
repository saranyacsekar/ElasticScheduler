package SchIQP.CustSch;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
    	Cluster.setNumNodes();   
    	QueryScheduler schObj=new QueryScheduler();
	    QueryScheduler.cur_time=1.0f;
    	if(args.length==0 || args.length>2){
			System.out.println("Improper inputs");
			System.exit(-1);
		}
		else if(args.length==1){
			if(args[0].equalsIgnoreCase("SimLLF")){
				schObj.sim_llf();
			}
			else if(args[0].equalsIgnoreCase("SimFC")){
				schObj.sim_fixed_config();
			}
			else if(args[0].equalsIgnoreCase("SimEC")){
			   schObj.sim_elastic_config();
			}
			else{
			System.out.println("Improper inputs");
			System.exit(-1);
			}
		}
		else if(args.length==2 && args[0].equalsIgnoreCase("Exp")){		
			CSSparkSession.create_spark_session("20","4");	      
	        schObj.dyn_sch_main(args[1]);
		}
		else{
			System.out.println("Improper inputs");
			System.exit(-1);
		}
        
    }
}
