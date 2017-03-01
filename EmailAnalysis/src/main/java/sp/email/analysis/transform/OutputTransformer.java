package sp.email.analysis.transform;

import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.StructType;



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

            String query = " select t2.message_id,t2.sender,t2.subject,t1.sender,abs(t2.email_date-t1.email_date) as response_time " +
                    "from email t1 " +
                    "left outer join email t2 on t1.hash = t2.hash and t1.message_id != t2.message_id and t1.sender != t2.sender " +
                    "where t2.hash is not null order by response_time";


            Row[] third = hqlc.sql(query).limit(5).collect();

            System.out.println("The nums of rows for third is :: " + third.length);

            printRows(third);

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


    public static void printRows(Row[] rows){

        for(Row row: rows){
            System.out.println(row.mkString(" "));
        }

    }
}

