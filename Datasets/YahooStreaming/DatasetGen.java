import java.io.*;
import java.util.*;
import java.util.concurrent.*;

public class DatasetGen {

    private static final int NUM_CAMPAIGNS = 100;
    private static final int VIEW_CAPACITY_PER_WINDOW = 10;
    private static final long KAFKA_EVENT_COUNT = 150_000_000L; // 100 millions
    private static final long TIME_DIVISOR = 10000L; // 10 seconds
    
    private static final String CAMPAIGN_IDS_FILE = "campaign-ids.txt";
    private static final String AD_IDS_FILE = "ad-ids.txt";
    private static final String AD_TO_CAMPAIGN_IDS_FILE = "ad-to-campaign-ids.txt";
    private static final String KAFKA_JSON_FILE = "kafka-json.txt";
    private static final String SEEN_FILE = "seen.txt";
    private static final String UPDATED_FILE = "updated.txt";
    
    private static final String[] AD_TYPES = {"banner", "modal", "sponsored-search", "mail", "mobile"};
    private static final String[] EVENT_TYPES = {"view", "click", "purchase"};
    
  
    private static final Random random = new Random();
    
public static void writeIds(List<String> campaigns, List<String> ads) {
    try (FileWriter campaignWriter = new FileWriter(CAMPAIGN_IDS_FILE);
         FileWriter adsWriter = new FileWriter(AD_IDS_FILE)) {
        
        for (String campaign : campaigns) {
            campaignWriter.write(campaign + "\n");                
        }
        
        for (String ad : ads) {
            adsWriter.write(ad + "\n");
        }
    } catch (IOException e) {
        System.err.println("Error writing IDs to files: " + e.getMessage());
        e.printStackTrace();
    }
}

public static List<String> makeIds(int n) {
    List<String> ids = new ArrayList<>();
    for (int i = 0; i < n; i++) {
        ids.add(UUID.randomUUID().toString());
    }
    return ids;
}

public void  GenCampAdIds() {
	List<String> campaigns = makeIds(NUM_CAMPAIGNS);
    List<String> ads = makeIds(NUM_CAMPAIGNS * 10);
        writeIds(campaigns, ads);
     writeToRedis(campaigns, ads);
	System.out.println("Created " + NUM_CAMPAIGNS + " campaigns and " + 
                (NUM_CAMPAIGNS * 10) + " ads.");
}

public static void writeToRedis(List<String> campaigns, List<String> ads) {
  
    try {    
	FileWriter adToCampaignWriter = new FileWriter(AD_TO_CAMPAIGN_IDS_FILE);
        
                   
        
        // Partition ads into groups of 10 per campaign
        for (int i = 0; i < campaigns.size(); i++) {
            String campaign = campaigns.get(i);
            int startIdx = i * 10;
            int endIdx = Math.min(startIdx + 10, ads.size());
            
                        
            for (int j = startIdx; j < endIdx; j++) {
                String ad = ads.get(j);
                String jsonEntry = String.format("{ \"%s\": \"%s\"}", ad, campaign);
                adToCampaignWriter.write(jsonEntry + "\n");
        
            }
        }
    } catch (IOException e) {
        System.err.println("Error writing to Redis: " + e.getMessage());
        e.printStackTrace();
    } finally {
       
    }
}

public static void writeToKafka(List<String> ads,int eventsCtr) {
   
    
    try {
        
        long startTime = System.currentTimeMillis();
        long skew = 0;
        long lateBy = 0;
        
        List<String> userIds = makeIds((int) KAFKA_EVENT_COUNT);
        List<String> pageIds = makeIds((int) KAFKA_EVENT_COUNT);
        String jsonObj="";
        int fileCtr=1;
	int ctr=1;

	String EVENTS_FILE="EVENTS_FILE"+"-"+fileCtr+".txt";
        FileWriter EVENTS_FILE_PTR = new FileWriter(EVENTS_FILE);
        for (long n = 0; n < KAFKA_EVENT_COUNT; n++) {
            
            if(ctr==1){
                   // jsonObj="{\"Events\":[";
		   }
            if (n % 10000 == 0) {
                //System.out.println(n);
            }
            
            String userId = userIds.get((int) (n % userIds.size()));
            String pageId = pageIds.get((int) (n % pageIds.size()));
            String adId = ads.get(random.nextInt(ads.size()));
            String adType = AD_TYPES[random.nextInt(AD_TYPES.length)];
            String eventType = EVENT_TYPES[random.nextInt(EVENT_TYPES.length)];
            long eventTime = startTime + (n * 10) + skew + lateBy;
            
            String jsonStr = String.format(
                "{\"user_id\": \"%s\", \"page_id\": \"%s\", \"ad_id\": \"%s\", " +
                "\"ad_type\": \"%s\", \"event_type\": \"%s\", \"event_time\": \"%d\", " +
                "\"ip_address\": \"1.2.3.4\"}",
                userId, pageId, adId, adType, eventType, eventTime
            );
            
            //System.out.println(jsonStr + "\n");
            jsonObj=jsonStr;

            if(ctr==eventsCtr){
                //jsonObj+="]}";
                EVENTS_FILE_PTR.append(jsonObj);
		EVENTS_FILE_PTR.flush();
		EVENTS_FILE_PTR.close();
                fileCtr++;
                EVENTS_FILE = "EVENTS_FILE"+"-"+fileCtr+".txt";
                EVENTS_FILE_PTR = new FileWriter(EVENTS_FILE);
		System.out.println(fileCtr);
		//moveFiles(EVENTS_FILE);
		ctr=1;
            }
            else{
                jsonObj+=",";                
                EVENTS_FILE_PTR.append(jsonObj);
	        ctr++;
            }
            
        }            
        
    } catch (Exception e) {
        System.err.println("Exception: " + e.getMessage());
        e.printStackTrace();
    }
}

/**
 * Generate ads and store in Redis
 */
public static List<String> genAds(List<String> campaigns) {
  
    
    try  {
       
        
        if (campaigns.size() < NUM_CAMPAIGNS) {
            throw new RuntimeException("No Campaigns found. Please run with -n first.");
        }
        
        List<String> ads = makeIds(NUM_CAMPAIGNS * 10);
        
        int adIndex = 0;
        for (String campaign : campaigns) {
            for (int i = 0; i < 10 && adIndex < ads.size(); i++) {                   
                adIndex++;
            }
        }
        
        return ads;
    } finally {
       
    }
}

/**
 * Load IDs from files
 */
public static Map<String, List<String>> loadIds() {
    Map<String, List<String>> result = new HashMap<>();
    List<String> campaigns = new ArrayList<>();
    List<String> ads = new ArrayList<>();
    
    try (BufferedReader campaignReader = new BufferedReader(new FileReader(CAMPAIGN_IDS_FILE));
         BufferedReader adsReader = new BufferedReader(new FileReader(AD_IDS_FILE))) {
        
        String line;
        while ((line = campaignReader.readLine()) != null) {
            campaigns.add(line);
        }
        
        while ((line = adsReader.readLine()) != null) {
            ads.add(line);
        }
        
        result.put("campaigns", campaigns);
        result.put("ads", ads);
    } catch (FileNotFoundException e) {
        System.out.println("Failed to load ids from file.");
    } catch (IOException e) {
        System.err.println("Error reading ID files: " + e.getMessage());
        e.printStackTrace();
    }
    
    return result;
}

public static void run(int throughput, boolean withSkew, String kafkaHosts, String redisHost) {
    System.out.println("Running, emitting " + throughput + " tuples per second.");
    
   
    
   /* try  {
        List<String> ads = genAds(redisHost);
        List<String> pageIds = makeIds(100);
        List<String> userIds = makeIds(100);
        
        long startTimeNs = System.currentTimeMillis() * 1_000_000L;
        long periodNs = 1_000_000_000L / throughput;
        
        for (long i = 0; ; i++) {
            long t = startTimeNs + (periodNs * i);
            long curMs = System.currentTimeMillis();
            long tMs = t / 1_000_000L;
            
            if (tMs > curMs) {
                Thread.sleep(tMs - curMs);
            } else if (curMs > (tMs + 100)) {
                System.out.println("Falling behind by: " + (curMs - tMs) + " ms");
            }
            
            
        }
    } catch (InterruptedException e) {
        System.out.println("Producer interrupted.");
        Thread.currentThread().interrupt();
    }*/
}

public void GenEvents(){
    // Generate Kafka events
    Map<String, List<String>> loaded = loadIds();
    if (loaded.containsKey("ads") && !loaded.get("ads").isEmpty()) {
        writeToKafka(loaded.get("ads"),40000);
        System.out.println("Generated Kafka events.");
    } else {
        System.out.println("No ads found. Please run with -n first.");
    }
}

 public static void moveFiles(String files) {
	      Process proc;
      	      String command = "aws s3 mv "+files+" s3://cs-spark-emr-s3/yahoodataset/jsoneventsnew/";
      	      System.out.println(command);
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

public static void main(String args[]) {
	DatasetGen obj=new DatasetGen();
	obj.GenCampAdIds();
    obj.GenEvents();
	
}
}
