package sp.email.analysis.transform;

import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.StructType;
import sp.email.analysis.utils.Constants;


/**
 * Created by sahityapavurala on 2/25/17.
 * OutputTransformer class implementing the Transformer interface
 * to output the results to stdout
 */
public class OutputTransformer implements Transformer{
    public static Logger LOGGER = Logger.getLogger(OutputTransformer.class);

    private String emailSource;
    private String recipientSource;
    private StructType emailSchema;
    private StructType recipientSchema;

    /**
     * Override transform method of Transformer interface
     * @param hqlc
     * @param sc
     */
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


            Row[] first = hqlc.sql(Constants.CREATE_RECIPIENTS).limit(3).collect();
            System.out.println("The top three recipients of direct emails are ");
            printRows(first);

            Row[] second = hqlc.sql(Constants.CREATE_SENDERS).limit(3).collect();
            System.out.println("The top three senders of broadcast emails are ");
            printRows(second);

            Row[] third = hqlc.sql(Constants.CREATE_RESPONSE_TIMES).limit(5).collect();
            System.out.println("The nums of rows for third is :: " + third.length);
            printRows(third);

        }
        catch (Exception e) {
            LOGGER.info("Exception in transform :: " + e.toString());
            e.printStackTrace();
        }

    }

    /**
     * Setter method to set sources of the csv files
     * @param emalPath
     * @param recipientPath
     */
    public void setSources(Path emalPath, Path recipientPath) {
        this.emailSource = emalPath.toString();
        this.recipientSource = recipientPath.toString();
    }

    /**
     * Setter method to set schema of data frames
     * @param emailSchema
     * @param recipientSchema
     */
    public void setSchema(StructType emailSchema, StructType recipientSchema){

        this.emailSchema = emailSchema;
        this.recipientSchema = recipientSchema;

    }

    /**
     * Utility method to print results
     * @param rows
     */
    public static void printRows(Row[] rows){

        for(Row row: rows){
            System.out.println(row.mkString(" "));
        }

    }
}

