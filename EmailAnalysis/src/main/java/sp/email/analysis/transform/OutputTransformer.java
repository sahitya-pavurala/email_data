package sp.email.analysis.transform;

import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.StructType;
import sp.email.analysis.utils.FilePreprocessor;

import java.util.*;

/**
 * Created by sahityapavurala on 2/25/17.
 */
public class OutputTransformer {
    public static Logger LOGGER = Logger.getLogger(OutputTransformer.class);

    private String emailSource;
    private String recipientSource;
    private StructType emailSchema;
    private StructType recipientSchema;


    public void transform(HiveContext hqlc, SparkContext sc){
        try {
            DataFrame emailDF = hqlc.read()
                    .format("com.databricks.spark.csv")
                    .schema(emailSchema)
                    .load(emailSource);

            emailDF.registerTempTable("email");

            DataFrame recipientDF = hqlc.read()
                    .format("com.databricks.spark.csv")
                    .schema(recipientSchema)
                    .load(recipientSource);

            recipientDF.registerTempTable("recipient");


            Row[] first = hqlc.sql("select recipient,count(e.message_id) as cnt from recipient r inner join email e on e.message_id = r.message_id  " +
                    "where e.label = 'direct' " +
                    "group by recipient order by cnt desc").limit(3).collect();
            System.out.println("The top three recipients of direct emails are ");
            printRows(first);

            Row[] second = hqlc.sql("select sender,count(*) as cnt from email where label='broadcast' " +
                    "group by sender order by cnt desc").limit(3).collect();
            System.out.println("The top three senders of broadcast emails are ");
            printRows(second);


            Row[] third = hqlc.sql(" select res.sender,res.response_time" +
                    " from(" +
                    " select t2.message_id,t2.hash,t2.sender,t2.subject,t2.email_date-t1.email_date as response_time" +
                    " from email t1" +
                    " inner join email t2 on t1.hash = t2.hash and t1.message_id != t2.message_id" +
                    " left join recipient t3 on t2.sender = t3.recipient and t2.message_id = t3.message_id" +
                    " order by response_time" +
                    " ) res").limit(5).collect();

            System.out.println("The nums of rows for third is :: " + third.length);

            printRows(third);
/**
 * SELECT
 start_log.name,
 MAX(start_log.ts) AS start_time,
 end_log.ts AS end_time,
 TIMEDIFF(MAX(start_log.ts), end_log.ts)
 FROM
 log AS start_log
 INNER JOIN
 log AS end_log ON (
 start_log.name = end_log.name
 AND
 end_log.ts > start_log.ts)
 WHERE start_log.eventtype = 'start'
 AND end_log.eventtype = 'stop'
 GROUP BY start_log.name

 select res.sender,res.response_time
 from(
 select t2.message_id,t2.hash,t2.sender,t2.subject,
 case when (t2.email_date > t1.email_date ) then (t2.email_date-t1.email_date)
 else end
 )as response_time
 from email t1
 inner join email t2 on t1.hash = t2.hash and t1.message_id != t2.message_id
 inner join recipient t3 on t2.sender = t3.recipient
  order by response_time
 ) res


 */



            HashMap<String, Long> response = new HashMap<String, Long>();
            String preHash = null;
            Long pretimeStamp = null;

            for (Row row : third) {
                String hash = (String) row.get(0);
                String sender = (String) row.get(1);
                String message_id = (String) row.get(2);
                Long email_date = (Long) row.get(3);

                if (hash != preHash) {
                    preHash = hash;
                    pretimeStamp = email_date;
                    continue;
                } else {
                    preHash = hash;
                            //+ "," + subject + "," + sender + "," + recipient;
                    response.put(message_id, email_date - pretimeStamp);
                    pretimeStamp = email_date;
                }

            }

            System.out.println("The size of map is :: " + response.size());

            Map<String, Long> sortedMap = sortByValue(response);

            System.out.println("The size of sorted map is :: " + sortedMap.size());

            printMap(sortedMap, 5);
        }
        catch (Exception e) {
            LOGGER.info("Exception in transform :: " + e.toString());
            e.printStackTrace();
        }

    }


    public void setSources(Path emalPath, Path recipientPath) {
        this.emailSource = emalPath.toString();
        this.recipientSource = recipientPath.toString();
    }

    public void setSchema(StructType emailSchema, StructType recipientSchema){

        this.emailSchema = emailSchema;
        this.recipientSchema = recipientSchema;

    }

    public static <K, V extends Comparable<? super V>> Map<K, V> sortByValue(Map<K, V> unsortMap) {

        List<Map.Entry<K, V>> list =
                new LinkedList<Map.Entry<K, V>>(unsortMap.entrySet());

        Collections.sort(list, new Comparator<Map.Entry<K, V>>() {
            public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
                return (o1.getValue()).compareTo(o2.getValue());
            }
        });

        Map<K, V> result = new LinkedHashMap<K, V>();
        for (Map.Entry<K, V> entry : list) {
            result.put(entry.getKey(), entry.getValue());
        }

        return result;

    }

    public static <K, V> void printMap(Map<K, V> map,int num) {
        int count = 0;
        System.out.println("The Fastest response times are ::");
        try {
            for (Map.Entry<K, V> entry : map.entrySet()) {
                System.out.println("Printing inside sorted map");
                if (count < num) {
                    System.out.println("Key : " + entry.getKey() + " Value : " + entry.getValue());
                    count += 1;
                } else
                    break;
            }
        }
        catch (Exception e){
            System.out.println("Exception in printing sorted map :: " + e.toString());
            e.printStackTrace();
        }

    }

    public static void printRows(Row[] rows){

        for(Row row: rows){
            System.out.println(row.mkString(" "));
        }

    }
}

